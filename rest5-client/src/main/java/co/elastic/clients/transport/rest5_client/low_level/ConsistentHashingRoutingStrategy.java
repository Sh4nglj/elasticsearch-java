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

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Consistent hashing request routing strategy.
 */
public class ConsistentHashingRoutingStrategy implements RequestRoutingStrategy {
    
    private static final int VIRTUAL_NODES = 100;
    private static final String HASH_ALGORITHM = "SHA-256";
    
    private final RoutingStats stats = new RoutingStats();
    private final ConcurrentMap<Long, Node> hashRing = new ConcurrentHashMap<>();
    private final SortedMap<Long, Node> sortedHashRing = Collections.synchronizedSortedMap(new TreeMap<>());
    
    @Override
    public Iterator<Node> selectNodes(
        List<Node> nodes,
        ConcurrentMap<HttpHost, DeadHostState> blacklist,
        AtomicInteger lastNodeIndex,
        NodeSelector nodeSelector
    ) throws IOException {
        /*
         * Update the hash ring if nodes have changed
         */
        updateHashRing(nodes, blacklist);
        
        /*
         * Normal state: there is at least one living node. If the
         * selector is ok with any over the living nodes then use them
         * for the request.
         */
        List<Node> livingNodes = new ArrayList<>(sortedHashRing.values());
        List<Node> selectedLivingNodes = new ArrayList<>(livingNodes);
        nodeSelector.select(selectedLivingNodes);
        
        if (!selectedLivingNodes.isEmpty()) {
            /*
             * For consistent hashing, we need a key from the request. However,
             * since we don't have access to the request body here, we'll use
             * a combination of timestamp and thread ID as a fallback.
             * In a real implementation, we would extract a key from the request.
             */
            String key = System.currentTimeMillis() + ":" + Thread.currentThread().getId();
            long hash = hash(key);
            
            // Find the first node in the ring with hash >= key hash
            SortedMap<Long, Node> tailMap = sortedHashRing.tailMap(hash);
            Node selectedNode;
            if (tailMap.isEmpty()) {
                // Wrap around to the beginning
                selectedNode = sortedHashRing.get(sortedHashRing.firstKey());
            } else {
                selectedNode = tailMap.get(tailMap.firstKey());
            }
            
            return Collections.singletonList(selectedNode).iterator();
        }

        throw new IOException("NodeSelector [" + nodeSelector + "] rejected all nodes");
    }
    
    private void updateHashRing(List<Node> nodes, ConcurrentMap<HttpHost, DeadHostState> blacklist) {
        // Clear existing ring
        hashRing.clear();
        sortedHashRing.clear();
        
        // Add living nodes to the ring with virtual nodes
        for (Node node : nodes) {
            DeadHostState deadness = blacklist.get(node.getHost());
            if (deadness == null || deadness.shallBeRetried()) {
                for (int i = 0; i < VIRTUAL_NODES; i++) {
                    String virtualNodeKey = node.getHost().toString() + ":" + i;
                    long hash = hash(virtualNodeKey);
                    hashRing.put(hash, node);
                    sortedHashRing.put(hash, node);
                }
            }
        }
    }
    
    private long hash(String key) {
        try {
            MessageDigest md = MessageDigest.getInstance(HASH_ALGORITHM);
            byte[] hashBytes = md.digest(key.getBytes());
            // Convert to long (use first 8 bytes)
            long hash = 0;
            for (int i = 0; i < Math.min(8, hashBytes.length); i++) {
                hash = (hash << 8) | (hashBytes[i] & 0xFF);
            }
            return hash;
        } catch (NoSuchAlgorithmException e) {
            throw new RuntimeException("Hash algorithm not found", e);
        }
    }
    
    @Override
    public void onRequestSuccess(Node node) {
        stats.incrementTotalRequests();
        stats.incrementTotalSuccesses();
        NodeMetrics metrics = stats.getNodeMetrics(node.getHost());
        metrics.incrementRequestCount();
        metrics.incrementSuccessfulRequests();
    }
    
    @Override
    public void onRequestFailure(Node node) {
        stats.incrementTotalRequests();
        stats.incrementTotalFailures();
        NodeMetrics metrics = stats.getNodeMetrics(node.getHost());
        metrics.incrementRequestCount();
        metrics.incrementFailedRequests();
        metrics.addError();
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