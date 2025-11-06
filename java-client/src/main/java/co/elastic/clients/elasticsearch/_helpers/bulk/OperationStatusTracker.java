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

import co.elastic.clients.elasticsearch.core.bulk.BulkOperation;
import co.elastic.clients.elasticsearch.core.bulk.BulkResponseItem;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Operation status tracker for transactional bulk operations.
 * Tracks the original state of each operation and its execution result.
 */
public class OperationStatusTracker<Context> {
    private final ConcurrentMap<Long, TrackedOperation<Context>> operations = new ConcurrentHashMap<>();
    private final AtomicLong operationIdGenerator = new AtomicLong();
    
    /**
     * Track a new operation.
     *
     * @param operation the bulk operation
     * @param context the context associated with the operation
     * @return the operation ID
     */
    public long trackOperation(BulkOperation operation, Context context) {
        long operationId = operationIdGenerator.incrementAndGet();
        TrackedOperation<Context> trackedOperation = new TrackedOperation<>(operationId, operation, context);
        operations.put(operationId, trackedOperation);
        return operationId;
    }
    
    /**
     * Mark an operation as successful.
     *
     * @param operationId the operation ID
     * @param responseItem the bulk response item
     */
    public void markOperationSuccess(long operationId, BulkResponseItem responseItem) {
        TrackedOperation<Context> trackedOperation = operations.get(operationId);
        if (trackedOperation != null) {
            trackedOperation.markSuccess(responseItem);
        }
    }
    
    /**
     * Mark an operation as failed.
     *
     * @param operationId the operation ID
     * @param responseItem the bulk response item
     */
    public void markOperationFailed(long operationId, BulkResponseItem responseItem) {
        TrackedOperation<Context> trackedOperation = operations.get(operationId);
        if (trackedOperation != null) {
            trackedOperation.markFailed(responseItem);
        }
    }
    
    /**
     * Get all tracked operations.
     *
     * @return the list of tracked operations
     */
    public List<TrackedOperation<Context>> getAllOperations() {
        return new ArrayList<>(operations.values());
    }
    
    /**
     * Get all successful operations.
     *
     * @return the list of successful operations
     */
    public List<TrackedOperation<Context>> getSuccessfulOperations() {
        List<TrackedOperation<Context>> result = new ArrayList<>();
        for (TrackedOperation<Context> operation : operations.values()) {
            if (operation.isSuccess()) {
                result.add(operation);
            }
        }
        return result;
    }
    
    /**
     * Get all failed operations.
     *
     * @return the list of failed operations
     */
    public List<TrackedOperation<Context>> getFailedOperations() {
        List<TrackedOperation<Context>> result = new ArrayList<>();
        for (TrackedOperation<Context> operation : operations.values()) {
            if (operation.isFailed()) {
                result.add(operation);
            }
        }
        return result;
    }
    
    /**
     * Clear all tracked operations.
     */
    public void clear() {
        operations.clear();
    }
    
    /**
     * Tracked operation with status.
     */
    public static class TrackedOperation<Context> {
        private final String operationId;
        private final BulkOperation operation;
        private final Context context;
        private final Instant createdAt;
        private volatile OperationStatus status;
        private volatile Instant statusUpdatedAt;

        public TrackedOperation(String operationId, BulkOperation operation, Context context) {
            this.operationId = operationId;
            this.operation = operation;
            this.context = context;
            this.createdAt = Instant.now();
            this.status = OperationStatus.PENDING;
            this.statusUpdatedAt = Instant.now();
        }

        // Getters and setters
        public String getOperationId() { return operationId; }
        public BulkOperation getOperation() { return operation; }
        public Context getContext() { return context; }
        public Instant getCreatedAt() { return createdAt; }
        public OperationStatus getStatus() { return status; }
        public Instant getStatusUpdatedAt() { return statusUpdatedAt; }

        public void setStatus(OperationStatus status) {
            this.status = status;
            this.statusUpdatedAt = Instant.now();
        }
    }
}