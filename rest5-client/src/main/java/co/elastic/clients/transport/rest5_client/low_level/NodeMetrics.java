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

import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.DoubleAdder;

/**
 * Holds performance metrics for a node.
 */
public class NodeMetrics {
    
    private final DoubleAdder cpuUsage = new DoubleAdder();
    private final DoubleAdder memoryUsage = new DoubleAdder();
    private final AtomicLong requestCount = new AtomicLong();
    private final AtomicLong successfulRequests = new AtomicLong();
    private final AtomicLong failedRequests = new AtomicLong();
    private final DoubleAdder totalResponseTime = new DoubleAdder();
    private final AtomicLong activeConnections = new AtomicLong();
    private final DoubleAdder errorRate = new DoubleAdder();
    
    // For weighted strategies
    private volatile double weight = 1.0;
    
    public double getCpuUsage() {
        return cpuUsage.sum();
    }
    
    public void addCpuUsage(double usage) {
        cpuUsage.add(usage);
    }
    
    public double getMemoryUsage() {
        return memoryUsage.sum();
    }
    
    public void addMemoryUsage(double usage) {
        memoryUsage.add(usage);
    }
    
    public long getRequestCount() {
        return requestCount.get();
    }
    
    public void incrementRequestCount() {
        requestCount.incrementAndGet();
    }
    
    public long getSuccessfulRequests() {
        return successfulRequests.get();
    }
    
    public void incrementSuccessfulRequests() {
        successfulRequests.incrementAndGet();
    }
    
    public long getFailedRequests() {
        return failedRequests.get();
    }
    
    public void incrementFailedRequests() {
        failedRequests.incrementAndGet();
    }
    
    public double getAverageResponseTime() {
        long count = requestCount.get();
        return count > 0 ? totalResponseTime.sum() / count : 0;
    }
    
    public void addResponseTime(long timeMillis) {
        totalResponseTime.add(timeMillis);
    }
    
    public long getActiveConnections() {
        return activeConnections.get();
    }
    
    public void incrementActiveConnections() {
        activeConnections.incrementAndGet();
    }
    
    public void decrementActiveConnections() {
        activeConnections.decrementAndGet();
    }
    
    public double getErrorRate() {
        long count = requestCount.get();
        return count > 0 ? errorRate.sum() / count : 0;
    }
    
    public void addError() {
        errorRate.add(1.0);
    }
    
    public double getWeight() {
        return weight;
    }
    
    public void setWeight(double weight) {
        this.weight = weight;
    }
    
    public void updateWeightBasedOnMetrics() {
        // Simple weight calculation based on performance metrics
        // This should be improved with more sophisticated algorithms
        double cpuFactor = Math.max(0.1, 1.0 - (getCpuUsage() / 100.0));
        double memoryFactor = Math.max(0.1, 1.0 - (getMemoryUsage() / 100.0));
        double errorFactor = Math.max(0.1, 1.0 - getErrorRate());
        double responseTimeFactor = Math.max(0.1, 1.0 / (1.0 + getAverageResponseTime() / 100.0));
        
        this.weight = cpuFactor * memoryFactor * errorFactor * responseTimeFactor;
        
        // Normalize weight to be between 0.1 and 10.0
        this.weight = Math.max(0.1, Math.min(10.0, this.weight));
    }
    
    @Override
    public String toString() {
        return "NodeMetrics{" +
            "cpuUsage=" + String.format("%.2f", getCpuUsage()) +
            ", memoryUsage=" + String.format("%.2f", getMemoryUsage()) +
            ", requestCount=" + requestCount +
            ", successfulRequests=" + successfulRequests +
            ", failedRequests=" + failedRequests +
            ", averageResponseTime=" + String.format("%.2f", getAverageResponseTime()) +
            ", activeConnections=" + activeConnections +
            ", errorRate=" + String.format("%.2f", getErrorRate()) +
            ", weight=" + String.format("%.2f", weight) +
            '}';
    }
}