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

import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Interface for request routing strategies. Defines how nodes are selected for request execution.
 */
public interface RequestRoutingStrategy {
    
    /**
     * Selects nodes to try for a request.
     * @param nodes All available nodes
     * @param blacklist Blacklisted nodes with their dead state
     * @param lastNodeIndex Atomic counter for round-robin selection
     * @param nodeSelector Node selector for filtering nodes
     * @return An iterator of nodes to try in order
     */
    Iterator<Node> selectNodes(
        List<Node> nodes,
        ConcurrentMap<HttpHost, DeadHostState> blacklist,
        AtomicInteger lastNodeIndex,
        NodeSelector nodeSelector
    ) throws IOException;
    
    /**
     * Notifies the strategy of a successful request execution.
     * @param node The node that successfully handled the request
     */
    void onRequestSuccess(Node node);
    
    /**
     * Notifies the strategy of a failed request execution.
     * @param node The node that failed to handle the request
     */
    void onRequestFailure(Node node);
    
    /**
     * Updates node performance metrics.
     * @param node The node to update metrics for
     * @param metrics Performance metrics
     */
    void updateNodeMetrics(Node node, NodeMetrics metrics);
    
    /**
     * Gets the current routing statistics.
     * @return Routing statistics
     */
    RoutingStats getStats();
}