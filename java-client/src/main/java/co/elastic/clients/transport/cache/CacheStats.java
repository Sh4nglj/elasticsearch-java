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

import java.util.concurrent.atomic.AtomicLong;

/**
 * Cache statistics.
 */
public class CacheStats {
    private final AtomicLong hits = new AtomicLong(0);
    private final AtomicLong misses = new AtomicLong(0);
    private final AtomicLong evictions = new AtomicLong(0);
    private final AtomicLong size = new AtomicLong(0);
    private final AtomicLong requests = new AtomicLong(0);

    /**
     * Increment the hit count.
     */
    public void incrementHit() {
        hits.incrementAndGet();
        requests.incrementAndGet();
    }

    /**
     * Increment the miss count.
     */
    public void incrementMiss() {
        misses.incrementAndGet();
        requests.incrementAndGet();
    }

    /**
     * Increment the eviction count.
     */
    public void incrementEviction() {
        evictions.incrementAndGet();
    }

    /**
     * Increment the size by the given delta.
     */
    public void incrementSize(long delta) {
        size.addAndGet(delta);
    }

    /**
     * Decrement the size by the given delta.
     */
    public void decrementSize(long delta) {
        size.addAndGet(-delta);
    }

    /**
     * Get the number of cache hits.
     */
    public long getHits() {
        return hits.get();
    }

    /**
     * Get the number of cache misses.
     */
    public long getMisses() {
        return misses.get();
    }

    /**
     * Get the number of cache evictions.
     */
    public long getEvictions() {
        return evictions.get();
    }

    /**
     * Get the current cache size.
     */
    public long getSize() {
        return size.get();
    }

    /**
     * Get the total number of requests.
     */
    public long getRequests() {
        return requests.get();
    }

    /**
     * Get the cache hit rate.
     */
    public double getHitRate() {
        long total = requests.get();
        return total == 0 ? 0.0 : (double) hits.get() / total;
    }

    /**
     * Reset all statistics.
     */
    public void reset() {
        hits.set(0);
        misses.set(0);
        evictions.set(0);
        size.set(0);
        requests.set(0);
    }

    @Override
    public String toString() {
        return "CacheStats{" +
            "hits=" + hits +
            ", misses=" + misses +
            ", evictions=" + evictions +
            ", size=" + size +
            ", requests=" + requests +
            ", hitRate=" + String.format("%.2f%%", getHitRate() * 100) +
            '}';
    }
}