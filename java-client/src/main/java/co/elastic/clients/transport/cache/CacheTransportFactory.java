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

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.transport.Transport;

/**
 * Factory class to create ElasticsearchClient instances with caching capabilities.
 */
public class CacheTransportFactory {

    /**
     * Create an ElasticsearchClient with caching enabled using default cache configuration.
     *
     * @param transport The underlying transport to use
     * @return A new ElasticsearchClient with caching enabled
     */
    public static ElasticsearchClient createClientWithCache(Transport transport) {
        return createClientWithCache(transport, CacheConfig.DEFAULT);
    }

    /**
     * Create an ElasticsearchClient with caching enabled using the specified cache configuration.
     *
     * @param transport The underlying transport to use
     * @param config The cache configuration to use
     * @return A new ElasticsearchClient with caching enabled
     */
    public static ElasticsearchClient createClientWithCache(Transport transport, CacheConfig config) {
        Transport cacheTransport = new CacheTransport(transport, config);
        return new ElasticsearchClient(cacheTransport);
    }

    /**
     * Create a CacheTransport instance that wraps the given transport.
     *
     * @param transport The underlying transport to wrap
     * @return A new CacheTransport instance
     */
    public static CacheTransport createCacheTransport(Transport transport) {
        return new CacheTransport(transport);
    }

    /**
     * Create a CacheTransport instance that wraps the given transport with the specified configuration.
     *
     * @param transport The underlying transport to wrap
     * @param config The cache configuration to use
     * @return A new CacheTransport instance
     */
    public static CacheTransport createCacheTransport(Transport transport, CacheConfig config) {
        return new CacheTransport(transport, config);
    }
}