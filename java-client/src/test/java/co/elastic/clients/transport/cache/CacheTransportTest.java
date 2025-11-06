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
import co.elastic.clients.elasticsearch.core.SearchRequest;
import co.elastic.clients.elasticsearch.core.SearchResponse;
import co.elastic.clients.elasticsearch.core.search.Hit;
import co.elastic.clients.json.JsonpMapper;
import co.elastic.clients.json.jackson.JacksonJsonpMapper;
import co.elastic.clients.transport.Endpoint;
import co.elastic.clients.transport.Transport;
import co.elastic.clients.transport.TransportOptions;
import co.elastic.clients.transport.ElasticsearchTransport;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.io.IOException;
import java.util.Collections;
import java.util.concurrent.CompletableFuture;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

class CacheTransportTest {

    private Transport mockTransport;
    private ElasticsearchTransport mockElasticsearchTransport;
    private Endpoint<SearchRequest, SearchResponse<String>, ?> mockEndpoint;
    private SearchRequest searchRequest;
    private SearchResponse<String> searchResponse;
    private JsonpMapper jsonpMapper;

    @BeforeEach
    void setUp() {
        // Create mock transport
        mockTransport = mock(Transport.class);
        mockElasticsearchTransport = mock(ElasticsearchTransport.class);
        mockEndpoint = mock(Endpoint.class);
        jsonpMapper = new JacksonJsonpMapper();

        // Create test request and response
        searchRequest = SearchRequest.of(s -> s.index("test-index"));
        searchResponse = SearchResponse.of(r -> r.hits(h -> h.total(t -> t.value(1)).hits(Collections.singletonList(Hit.of(hi -> hi.source("{\"field\": \"value\"}"))))));

        // Set up mocks
        when(mockTransport.jsonpMapper()).thenReturn(jsonpMapper);
        when(mockTransport.options()).thenReturn(TransportOptions.DEFAULT);
        when(mockTransport.withOptions(any())).thenReturn(mockTransport);
        when(mockElasticsearchTransport.jsonpMapper()).thenReturn(jsonpMapper);
        when(mockElasticsearchTransport.options()).thenReturn(TransportOptions.DEFAULT);
        when(mockElasticsearchTransport.withOptions(any())).thenReturn(mockElasticsearchTransport);

        // Mock endpoint behavior
        when(mockEndpoint.id()).thenReturn("search");
        when(mockEndpoint.method(any())).thenReturn("GET");
        when(mockEndpoint.requestUrl(any(), any())).thenReturn("/test-index/_search");
        when(mockEndpoint.requestBodyRequired()).thenReturn(false);

        // Mock transport response
        when(mockTransport.performRequestAsync(eq(searchRequest), eq(mockEndpoint), any())).thenAnswer(invocation -> {
            CompletableFuture<SearchResponse<String>> future = new CompletableFuture<>();
            future.complete(searchResponse);
            return future;
        });
    }

    @Test
    void testRequestDeduplication() throws IOException, InterruptedException {
        // Create cache transport with short TTL for deduplication
        CacheConfig config = CacheConfig.builder()
                .enabled(true)
                .cacheType(CacheType.SHORT_TERM)
                .ttl(java.time.Duration.ofSeconds(1))
                .build();
        
        CacheTransport cacheTransport = new CacheTransport(mockTransport, config);

        // Send two identical requests
        CompletableFuture<SearchResponse<String>> future1 = cacheTransport.performRequestAsync(searchRequest, mockEndpoint, TransportOptions.DEFAULT);
        CompletableFuture<SearchResponse<String>> future2 = cacheTransport.performRequestAsync(searchRequest, mockEndpoint, TransportOptions.DEFAULT);

        // Both futures should complete with the same result
        SearchResponse<String> response1 = future1.get();
        SearchResponse<String> response2 = future2.get();

        assertEquals(response1, response2);
        assertSame(response1, response2); // Should be the exact same object

        // Verify transport was called only once
        verify(mockTransport, times(1)).performRequestAsync(eq(searchRequest), eq(mockEndpoint), any());

        // Check stats
        CacheStats stats = cacheTransport.getStats();
        assertEquals(1, stats.getMisses());
        assertEquals(1, stats.getHits());
        assertEquals(0, stats.getSize()); // Short-term cache doesn't store results
    }

    @Test
    void testLongTermCaching() throws IOException, InterruptedException {
        // Create cache transport with long-term caching
        CacheConfig config = CacheConfig.builder()
                .enabled(true)
                .cacheType(CacheType.LONG_TERM)
                .ttl(java.time.Duration.ofSeconds(10))
                .build();
        
        CacheTransport cacheTransport = new CacheTransport(mockTransport, config);

        // Send first request - should miss cache
        CompletableFuture<SearchResponse<String>> future1 = cacheTransport.performRequestAsync(searchRequest, mockEndpoint, TransportOptions.DEFAULT);
        SearchResponse<String> response1 = future1.get();

        // Send second request - should hit cache
        CompletableFuture<SearchResponse<String>> future2 = cacheTransport.performRequestAsync(searchRequest, mockEndpoint, TransportOptions.DEFAULT);
        SearchResponse<String> response2 = future2.get();

        assertEquals(response1, response2);

        // Verify transport was called only once
        verify(mockTransport, times(1)).performRequestAsync(eq(searchRequest), eq(mockEndpoint), any());

        // Check stats
        CacheStats stats = cacheTransport.getStats();
        assertEquals(1, stats.getMisses());
        assertEquals(1, stats.getHits());
        assertEquals(1, stats.getSize());
    }

    @Test
    void testWriteOperationNotCached() throws IOException, InterruptedException {
        // Create cache transport
        CacheTransport cacheTransport = new CacheTransport(mockTransport);

        // Mock a write operation (POST)
        Endpoint<?, ?, ?> writeEndpoint = mock(Endpoint.class);
        when(writeEndpoint.id()).thenReturn("index");
        when(writeEndpoint.method(any())).thenReturn("POST");
        when(writeEndpoint.requestUrl(any(), any())).thenReturn("/test-index/_doc");
        when(writeEndpoint.requestBodyRequired()).thenReturn(true);

        // Create a mock response for write operation
        Object writeResponse = new Object();
        when(mockTransport.performRequestAsync(any(), eq(writeEndpoint), any())).thenAnswer(invocation -> {
            CompletableFuture<Object> future = new CompletableFuture<>();
            future.complete(writeResponse);
            return future;
        });

        // Send two identical write requests
        Object request = new Object();
        CompletableFuture<?> future1 = cacheTransport.performRequestAsync(request, writeEndpoint, TransportOptions.DEFAULT);
        CompletableFuture<?> future2 = cacheTransport.performRequestAsync(request, writeEndpoint, TransportOptions.DEFAULT);

        // Both futures should complete
        future1.get();
        future2.get();

        // Verify transport was called twice (write operations are not cached)
        verify(mockTransport, times(2)).performRequestAsync(any(), eq(writeEndpoint), any());
    }

    @Test
    void testCacheInvalidation() throws IOException, InterruptedException {
        // Create cache transport with long-term caching
        CacheConfig config = CacheConfig.builder()
                .enabled(true)
                .cacheType(CacheType.LONG_TERM)
                .ttl(java.time.Duration.ofSeconds(10))
                .build();
        
        CacheTransport cacheTransport = new CacheTransport(mockTransport, config);

        // Send first request - should miss cache
        CompletableFuture<SearchResponse<String>> future1 = cacheTransport.performRequestAsync(searchRequest, mockEndpoint, TransportOptions.DEFAULT);
        future1.get();

        // Invalidate all cache
        cacheTransport.invalidateAll();

        // Send second request - should miss cache again
        CompletableFuture<SearchResponse<String>> future2 = cacheTransport.performRequestAsync(searchRequest, mockEndpoint, TransportOptions.DEFAULT);
        future2.get();

        // Verify transport was called twice
        verify(mockTransport, times(2)).performRequestAsync(eq(searchRequest), eq(mockEndpoint), any());

        // Check stats
        CacheStats stats = cacheTransport.getStats();
        assertEquals(2, stats.getMisses());
        assertEquals(1, stats.getHits()); // First hit was from deduplication
        assertEquals(0, stats.getSize()); // Cache was invalidated
    }

    @Test
    void testCacheStats() throws IOException, InterruptedException {
        // Create cache transport with long-term caching
        CacheConfig config = CacheConfig.builder()
                .enabled(true)
                .cacheType(CacheType.LONG_TERM)
                .ttl(java.time.Duration.ofSeconds(10))
                .build();
        
        CacheTransport cacheTransport = new CacheTransport(mockTransport, config);

        // Send first request - miss
        cacheTransport.performRequestAsync(searchRequest, mockEndpoint, TransportOptions.DEFAULT).get();

        // Send second request - hit
        cacheTransport.performRequestAsync(searchRequest, mockEndpoint, TransportOptions.DEFAULT).get();

        // Send third request - hit
        cacheTransport.performRequestAsync(searchRequest, mockEndpoint, TransportOptions.DEFAULT).get();

        CacheStats stats = cacheTransport.getStats();
        assertEquals(1, stats.getMisses());
        assertEquals(2, stats.getHits());
        assertEquals(1, stats.getSize());
        assertEquals(3, stats.getTotalRequests());
        assertEquals(66.67, stats.getHitRate(), 0.01);

        // Reset stats
        stats.reset();
        assertEquals(0, stats.getMisses());
        assertEquals(0, stats.getHits());
        assertEquals(0, stats.getTotalRequests());
    }
}