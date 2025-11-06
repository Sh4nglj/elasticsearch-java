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

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Holds routing statistics.
 */
public class RoutingStats {
    
    private final Map<HttpHost, NodeMetrics> nodeMetricsMap = new ConcurrentHashMap<>();
    private final long startTime = System.currentTimeMillis();
    private volatile long totalRequests = 0;
    private volatile long totalSuccesses = 0;
    private volatile long totalFailures = 0;
    
    public void incrementTotalRequests() {
        totalRequests++;
    }
    
    public void incrementTotalSuccesses() {
        totalSuccesses++;
    }
    
    public void incrementTotalFailures() {
        totalFailures++;
    }
    
    public long getTotalRequests() {
        return totalRequests;
    }
    
    public long getTotalSuccesses() {
        return totalSuccesses;
    }
    
    public long getTotalFailures() {
        return totalFailures;
    }
    
    public double getSuccessRate() {
        return totalRequests > 0 ? (double) totalSuccesses / totalRequests : 0;
    }
    
    public double getFailureRate() {
        return totalRequests > 0 ? (double) totalFailures / totalRequests : 0;
    }
    
    public long getUpTimeMillis() {
        return System.currentTimeMillis() - startTime;
    }
    
    public Map<HttpHost, NodeMetrics> getNodeMetrics() {
        return nodeMetricsMap;
    }
    
    public NodeMetrics getNodeMetrics(HttpHost host) {
        return nodeMetricsMap.computeIfAbsent(host, h -> new NodeMetrics());
    }
    
    public void updateNodeMetrics(HttpHost host, NodeMetrics metrics) {
        nodeMetricsMap.put(host, metrics);
    }
    
    @Override
    public String toString() {
        return "RoutingStats{" +
            "totalRequests=" + totalRequests +
            ", totalSuccesses=" + totalSuccesses +
            ", totalFailures=" + totalFailures +
            ", successRate=" + String.format("%.2f%%", getSuccessRate() * 100) +
            ", failureRate=" + String.format("%.2f%%", getFailureRate() * 100) +
            ", upTimeMillis=" + getUpTimeMillis() +
            ", nodeMetrics=" + nodeMetricsMap +
            '}';
    }
}