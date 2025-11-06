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

package co.elastic.clients.elasticsearch._helpers.bulk;

import java.util.concurrent.atomic.AtomicLong;

/**
 * Transaction metrics for bulk operations.
 */
public class TransactionMetrics {
    private final AtomicLong transactionCount = new AtomicLong();
    private final AtomicLong rollbackCount = new AtomicLong();
    private final AtomicLong rollbackSuccessCount = new AtomicLong();
    private final AtomicLong rollbackFailedCount = new AtomicLong();
    private final AtomicLong rollbackTotalTimeMillis = new AtomicLong();
    private final AtomicLong transactionTimeoutCount = new AtomicLong();
    
    /**
     * Increment the transaction count.
     */
    public void incrementTransactionCount() {
        transactionCount.incrementAndGet();
    }
    
    /**
     * Increment the rollback count.
     */
    public void incrementRollbackCount() {
        rollbackCount.incrementAndGet();
    }
    
    /**
     * Increment the rollback success count.
     */
    public void incrementRollbackSuccessCount() {
        rollbackSuccessCount.incrementAndGet();
    }
    
    /**
     * Increment the rollback failed count.
     */
    public void incrementRollbackFailedCount() {
        rollbackFailedCount.incrementAndGet();
    }
    
    /**
     * Add rollback time.
     * @param timeMillis the time in milliseconds
     */
    public void addRollbackTime(long timeMillis) {
        rollbackTotalTimeMillis.addAndGet(timeMillis);
    }
    
    /**
     * Increment the transaction timeout count.
     */
    public void incrementTransactionTimeoutCount() {
        transactionTimeoutCount.incrementAndGet();
    }
    
    // Getters for metrics
    public long getTransactionCount() {
        return transactionCount.get();
    }
    
    public long getRollbackCount() {
        return rollbackCount.get();
    }
    
    public long getRollbackSuccessCount() {
        return rollbackSuccessCount.get();
    }
    
    public long getRollbackFailedCount() {
        return rollbackFailedCount.get();
    }
    
    public long getRollbackTotalTimeMillis() {
        return rollbackTotalTimeMillis.get();
    }
    
    public long getTransactionTimeoutCount() {
        return transactionTimeoutCount.get();
    }
}