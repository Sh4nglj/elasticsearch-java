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

package co.elastic.clients.transport.cache;

import co.elastic.clients.json.JsonpMapper;
import co.elastic.clients.transport.Endpoint;
import co.elastic.clients.transport.Transport;
import co.elastic.clients.transport.TransportOptions;

import java.io.IOException;
import java.time.Instant;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.DelayQueue;
import java.util.concurrent.Delayed;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * A transport that adds caching and request deduplication capabilities to an existing transport.
 */
public class CacheTransport implements ElasticsearchTransport {
    private final Transport delegate;
    private final CacheConfig defaultConfig;
    private final RequestSigner signer;
    private final CacheStats stats;

    // Short-term cache for request deduplication (in-flight requests)
    private final ConcurrentMap<String, CompletableFuture<?>> inFlightRequests;
    
    // Long-term cache for completed requests
    private final ConcurrentMap<String, CacheEntry<?>> longTermCache;
    
    // Delay queue for cache entry expiration
    private final DelayQueue<ExpirationEntry> expirationQueue;
    
    // Lock for cache maintenance
    private final Lock maintenanceLock;

    public CacheTransport(Transport delegate) {
        this(delegate, CacheConfig.DEFAULT);
    }

    public CacheTransport(Transport delegate, CacheConfig defaultConfig) {
        this.delegate = delegate;
        this.defaultConfig = defaultConfig;
        this.signer = new RequestSigner(delegate.jsonpMapper());
        this.stats = new CacheStats();
        this.inFlightRequests = new ConcurrentHashMap<>();
        this.longTermCache = new ConcurrentHashMap<>();
        this.expirationQueue = new DelayQueue<>();
        this.maintenanceLock = new ReentrantLock();

        // Start expiration thread
        startExpirationThread();
    }

    @Override
    public <RequestT, ResponseT, ErrorT> ResponseT performRequest(
            RequestT request,
            Endpoint<RequestT, ResponseT, ErrorT> endpoint,
            TransportOptions options
    ) throws IOException {
        try {
            CompletableFuture<ResponseT> future = performRequestAsync(request, endpoint, options);
            return future.get();
        } catch (Exception e) {
            if (e instanceof IOException) {
                throw (IOException) e;
            }
            if (e.getCause() instanceof IOException) {
                throw (IOException) e.getCause();
            }
            throw new IOException("Cache transport error", e);
        }
    }

    @Override
    public <RequestT, ResponseT, ErrorT> CompletableFuture<ResponseT> performRequestAsync(
            RequestT request,
            Endpoint<RequestT, ResponseT, ErrorT> endpoint,
            TransportOptions options
    ) {
        try {
            String signature = signer.generateSignature(request, endpoint);
            
            // Check if this is a write operation (should not be cached)
            String method = endpoint.method(request);
            if (isWriteOperation(method)) {
                // Invalidate cache for related indices
                invalidateCacheForRequest(request, endpoint);
                // Forward to delegate without caching
                return delegate.performRequestAsync(request, endpoint, options);
            }

            // Check long-term cache first
            CacheEntry<?> entry = longTermCache.get(signature);
            if (entry != null && !entry.isExpired()) {
                stats.incrementHit();
                @SuppressWarnings("unchecked")
                CompletableFuture<ResponseT> future = CompletableFuture.completedFuture((ResponseT) entry.getValue());
                return future;
            }

            // Check if request is already in flight
            @SuppressWarnings("unchecked")
            CompletableFuture<ResponseT> existingFuture = (CompletableFuture<ResponseT>) inFlightRequests.get(signature);
            if (existingFuture != null && !existingFuture.isCompletedExceptionally()) {
                stats.incrementHit();
                return existingFuture;
            }

            // Create new future and add to in-flight requests
            CompletableFuture<ResponseT> newFuture = delegate.performRequestAsync(request, endpoint, options);
            
            // Store in in-flight requests
            CompletableFuture<?> previous = inFlightRequests.putIfAbsent(signature, newFuture);
            if (previous != null) {
                // Another thread already added it, use that one
                @SuppressWarnings("unchecked")
                CompletableFuture<ResponseT> prevFuture = (CompletableFuture<ResponseT>) previous;
                return prevFuture;
            }

            stats.incrementMiss();

            // When future completes, add to long-term cache and remove from in-flight
            newFuture.whenComplete((response, throwable) -> {
                inFlightRequests.remove(signature);
                
                if (throwable == null && response != null) {
                    // Add to long-term cache if enabled
                    if (defaultConfig.isEnabled() && defaultConfig.getCacheType() == CacheType.LONG_TERM) {
                        long ttlMillis = defaultConfig.getTtl().toMillis();
                        if (ttlMillis > 0) {
                            CacheEntry<ResponseT> cacheEntry = new CacheEntry<>(response, ttlMillis);
                            longTermCache.put(signature, cacheEntry);
                            stats.incrementSize(1);
                            
                            // Add to expiration queue
                            expirationQueue.put(new ExpirationEntry(signature, cacheEntry.getExpirationTime()));
                        }
                    }
                }
            });

            return newFuture;

        } catch (IOException e) {
            // Fallback to delegate if signature generation fails
            return delegate.performRequestAsync(request, endpoint, options);
        }
    }

    @Override
    public JsonpMapper jsonpMapper() {
        return delegate.jsonpMapper();
    }

    @Override
    public TransportOptions options() {
        return delegate.options();
    }

    @Override
    public Transport withOptions(TransportOptions options) {
        return new CacheTransport(delegate.withOptions(options), defaultConfig);
    }

    @Override
    public void close() throws IOException {
        delegate.close();
        // Clear all caches
        inFlightRequests.clear();
        longTermCache.clear();
        expirationQueue.clear();
    }

    /**
     * Check if a method is a write operation that should not be cached.
     */
    private boolean isWriteOperation(String method) {
        return method.equals("POST") || method.equals("PUT") || method.equals("DELETE") || method.equals("PATCH");
    }

    /**
     * Invalidate cache entries related to a write request.
     */
    private <RequestT, ResponseT, ErrorT> void invalidateCacheForRequest(
            RequestT request,
            Endpoint<RequestT, ResponseT, ErrorT> endpoint
    ) {
        // This is a simple implementation - in a real-world scenario, we'd extract index names from the request
        // and invalidate all cache entries related to those indices
        // For now, we'll clear the entire cache (simple but effective)
        longTermCache.clear();
        stats.incrementSize(-longTermCache.size());
    }

    /**
     * Start a thread to handle cache entry expiration.
     */
    private void startExpirationThread() {
        Thread expirationThread = new Thread(() -> {
            while (true) {
                try {
                    ExpirationEntry entry = expirationQueue.take();
                    if (entry == null) {
                        break;
                    }
                    
                    // Remove expired entry from cache
                    CacheEntry<?> cacheEntry = longTermCache.remove(entry.getSignature());
                    if (cacheEntry != null) {
                        stats.incrementEviction();
                        stats.decrementSize(1);
                    }
                } catch (InterruptedException e) {
                    // Exit thread
                    break;
                }
            }
        }, "CacheExpirationThread");
        expirationThread.setDaemon(true);
        expirationThread.start();
    }

    /**
     * Get cache statistics.
     */
    public CacheStats getStats() {
        return stats;
    }

    /**
     * Invalidate all cache entries.
     */
    public void invalidateAll() {
        longTermCache.clear();
        stats.incrementSize(-longTermCache.size());
    }

    /**
     * Invalidate cache entries for a specific request signature.
     */
    public void invalidate(String signature) {
        if (longTermCache.remove(signature) != null) {
            stats.decrementSize(1);
        }
    }

    // Internal classes
    private static class CacheEntry<T> {
        private final T value;
        private final long expirationTime;

        CacheEntry(T value, long ttlMillis) {
            this.value = value;
            this.expirationTime = Instant.now().toEpochMilli() + ttlMillis;
        }

        T getValue() {
            return value;
        }

        long getExpirationTime() {
            return expirationTime;
        }

        boolean isExpired() {
            return Instant.now().toEpochMilli() >= expirationTime;
        }
    }

    private static class ExpirationEntry implements Delayed {
        private final String signature;
        private final long expirationTime;

        ExpirationEntry(String signature, long expirationTime) {
            this.signature = signature;
            this.expirationTime = expirationTime;
        }

        String getSignature() {
            return signature;
        }

        @Override
        public long getDelay(TimeUnit unit) {
            long remaining = expirationTime - Instant.now().toEpochMilli();
            return unit.convert(remaining, TimeUnit.MILLISECONDS);
        }

        @Override
        public int compareTo(Delayed o) {
            if (o == this) {
                return 0;
            }
            long thisDelay = getDelay(TimeUnit.MILLISECONDS);
            long otherDelay = o.getDelay(TimeUnit.MILLISECONDS);
            return Long.compare(thisDelay, otherDelay);
        }
    }
}