/*
 * Licensed to Elasticsearch B.V. under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch B.V. licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package co.elastic.clients.transport.rest5_client.low_level;

import org.apache.hc.core5.http.HttpHost;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Least connections request routing strategy.
 */
public class LeastConnectionsRoutingStrategy implements RequestRoutingStrategy {
    
    private final RoutingStats stats = new RoutingStats();
    
    @Override
    public Iterator<Node> selectNodes(
        List<Node> nodes,
        ConcurrentMap<HttpHost, DeadHostState> blacklist,
        AtomicInteger lastNodeIndex,
        NodeSelector nodeSelector
    ) throws IOException {
        /*
         * Sort the nodes into living and dead lists.
         */
        List<Node> livingNodes = new ArrayList<>(Math.max(0, nodes.size() - blacklist.size()));
        List<Rest5Client.DeadNode> deadNodes = null;
        if (!blacklist.isEmpty()) {
            deadNodes = new ArrayList<>(blacklist.size());
            for (Node node : nodes) {
                DeadHostState deadness = blacklist.get(node.getHost());
                if (deadness == null || deadness.shallBeRetried()) {
                    livingNodes.add(node);
                } else {
                    deadNodes.add(new Rest5Client.DeadNode(node, deadness));
                }
            }
        } else {
            livingNodes.addAll(nodes);
        }

        if (!livingNodes.isEmpty()) {
            /*
             * Normal state: there is at least one living node. If the
             * selector is ok with any over the living nodes then use them
             * for the request.
             */
            List<Node> selectedLivingNodes = new ArrayList<>(livingNodes);
            nodeSelector.select(selectedLivingNodes);
            if (!selectedLivingNodes.isEmpty()) {
                /*
                 * Sort nodes by active connections (least connections first)
                 */
                selectedLivingNodes.sort(Comparator.comparingLong(node -> {
                    NodeMetrics metrics = stats.getNodeMetrics(node.getHost());
                    return metrics.getActiveConnections();
                }));
                
                // Increment active connections for the selected node
                if (!selectedLivingNodes.isEmpty()) {
                    Node selectedNode = selectedLivingNodes.get(0);
                    NodeMetrics metrics = stats.getNodeMetrics(selectedNode.getHost());
                    metrics.incrementActiveConnections();
                }
                
                return selectedLivingNodes.iterator();
            }
        }

        /*
         * Last resort: there are no good nodes to use, either because
         * the selector rejected all the living nodes or because there aren't
         * any living ones. Either way, we want to revive a single dead node
         * that the NodeSelectors are OK with. We do this by passing the dead
         * nodes through the NodeSelector so it can have its say in which nodes
         * are ok. If the selector is ok with any of the nodes then we will take
         * the one in the list that has the lowest revival time and try it.
         */
        if (deadNodes != null && !deadNodes.isEmpty()) {
            final List<Rest5Client.DeadNode> selectedDeadNodes = new ArrayList<>(deadNodes);
            /*
             * We'd like NodeSelectors to remove items directly from deadNodes
             * so we can find the minimum after it is filtered without having
             * to compare many things. This saves us a sort on the unfiltered
             * list.
             */
            nodeSelector.select(() -> new Rest5Client.DeadNodeIteratorAdapter(selectedDeadNodes.iterator()));
            if (!selectedDeadNodes.isEmpty()) {
                return Collections.singletonList(Collections.min(selectedDeadNodes).node).iterator();
            }
        }
        throw new IOException("NodeSelector [" + nodeSelector + "] rejected all nodes, living: " + livingNodes + " and dead: " + deadNodes);
    }
    
    @Override
    public void onRequestSuccess(Node node) {
        stats.incrementTotalRequests();
        stats.incrementTotalSuccesses();
        NodeMetrics metrics = stats.getNodeMetrics(node.getHost());
        metrics.incrementRequestCount();
        metrics.incrementSuccessfulRequests();
        metrics.decrementActiveConnections();
    }
    
    @Override
    public void onRequestFailure(Node node) {
        stats.incrementTotalRequests();
        stats.incrementTotalFailures();
        NodeMetrics metrics = stats.getNodeMetrics(node.getHost());
        metrics.incrementRequestCount();
        metrics.incrementFailedRequests();
        metrics.addError();
        metrics.decrementActiveConnections();
    }
    
    @Override
    public void updateNodeMetrics(Node node, NodeMetrics metrics) {
        stats.updateNodeMetrics(node.getHost(), metrics);
    }
    
    @Override
    public RoutingStats getStats() {
        return stats;
    }
}