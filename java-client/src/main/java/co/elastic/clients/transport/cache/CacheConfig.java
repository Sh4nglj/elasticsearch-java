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

import java.time.Duration;
import java.util.concurrent.TimeUnit;

/**
 * Cache configuration for Elasticsearch client requests.
 */
public class CacheConfig {
    private final Duration ttl;
    private final int maxSize;
    private final CacheEvictionPolicy evictionPolicy;
    private final boolean enabled;
    private final CacheType cacheType;

    private CacheConfig(Builder builder) {
        this.ttl = builder.ttl;
        this.maxSize = builder.maxSize;
        this.evictionPolicy = builder.evictionPolicy;
        this.enabled = builder.enabled;
        this.cacheType = builder.cacheType;
    }

    /**
     * Default cache configuration: enabled, 1 second TTL, 1000 entries, LRU eviction, short-term cache.
     */
    public static final CacheConfig DEFAULT = new Builder()
        .enabled(true)
        .ttl(Duration.ofSeconds(1))
        .maxSize(1000)
        .evictionPolicy(CacheEvictionPolicy.LRU)
        .cacheType(CacheType.SHORT_TERM)
        .build();

    public Duration getTtl() {
        return ttl;
    }

    public int getMaxSize() {
        return maxSize;
    }

    public CacheEvictionPolicy getEvictionPolicy() {
        return evictionPolicy;
    }

    public boolean isEnabled() {
        return enabled;
    }

    public CacheType getCacheType() {
        return cacheType;
    }

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder {
        private Duration ttl = Duration.ofSeconds(1);
        private int maxSize = 1000;
        private CacheEvictionPolicy evictionPolicy = CacheEvictionPolicy.LRU;
        private boolean enabled = true;
        private CacheType cacheType = CacheType.SHORT_TERM;

        public Builder ttl(Duration ttl) {
            this.ttl = ttl;
            return this;
        }

        public Builder ttl(long amount, TimeUnit unit) {
            this.ttl = Duration.ofMillis(unit.toMillis(amount));
            return this;
        }

        public Builder maxSize(int maxSize) {
            this.maxSize = maxSize;
            return this;
        }

        public Builder evictionPolicy(CacheEvictionPolicy evictionPolicy) {
            this.evictionPolicy = evictionPolicy;
            return this;
        }

        public Builder enabled(boolean enabled) {
            this.enabled = enabled;
            return this;
        }

        public Builder cacheType(CacheType cacheType) {
            this.cacheType = cacheType;
            return this;
        }

        public CacheConfig build() {
            return new CacheConfig(this);
        }
    }
}