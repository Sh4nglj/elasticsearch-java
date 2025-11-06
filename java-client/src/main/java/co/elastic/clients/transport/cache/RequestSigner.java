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
import co.elastic.clients.util.BinaryData;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Map;
import java.util.TreeMap;
import java.util.stream.Collectors;

/**
 * Generates unique signatures for Elasticsearch requests to enable caching and deduplication.
 */
public class RequestSigner {
    private static final String HASH_ALGORITHM = "SHA-256";
    private final JsonpMapper jsonpMapper;

    public RequestSigner(JsonpMapper jsonpMapper) {
        this.jsonpMapper = jsonpMapper;
    }

    /**
     * Generate a unique signature for a request and its endpoint.
     *
     * @param request the request object
     * @param endpoint the endpoint
     * @param <RequestT> request type
     * @param <ResponseT> response type
     * @param <ErrorT> error type
     * @return a unique signature string
     * @throws IOException if there's an error serializing the request
     */
    public <RequestT, ResponseT, ErrorT> String generateSignature(
            RequestT request,
            Endpoint<RequestT, ResponseT, ErrorT> endpoint
    ) throws IOException {
        try {
            MessageDigest digest = MessageDigest.getInstance(HASH_ALGORITHM);

            // Include endpoint ID
            updateDigest(digest, endpoint.id());

            // Include HTTP method
            updateDigest(digest, endpoint.method(request));

            // Include request URL
            updateDigest(digest, endpoint.requestUrl(request));

            // Include path parameters (sorted)
            Map<String, String> pathParams = new TreeMap<>(endpoint.pathParameters(request));
            for (Map.Entry<String, String> entry : pathParams.entrySet()) {
                updateDigest(digest, entry.getKey());
                updateDigest(digest, entry.getValue());
            }

            // Include query parameters (sorted)
            Map<String, String> queryParams = new TreeMap<>(endpoint.queryParameters(request));
            for (Map.Entry<String, String> entry : queryParams.entrySet()) {
                updateDigest(digest, entry.getKey());
                updateDigest(digest, entry.getValue());
            }

            // Include headers (sorted, excluding dynamic headers)
            Map<String, String> headers = new TreeMap<>(endpoint.headers(request));
            // Exclude headers that are not part of the request identity
            headers.entrySet().removeIf(entry -> {
                String key = entry.getKey().toLowerCase();
                return key.startsWith("x-elastic-client-") || key.equals("authorization") || key.equals("date");
            });
            for (Map.Entry<String, String> entry : headers.entrySet()) {
                updateDigest(digest, entry.getKey());
                updateDigest(digest, entry.getValue());
            }

            // Include request body
            Object body = endpoint.body(request);
            if (body != null) {
                if (body instanceof BinaryData) {
                    // For binary data, use the data directly
                    try {
                        digest.update(((BinaryData) body).toBytes());
                    } catch (IOException e) {
                        // Ignore error - this should not happen with BinaryData
                    }
                } else if (body instanceof Serializable) {
                    // For serializable objects, serialize them
                    try {
                        ByteArrayOutputStream baos = new ByteArrayOutputStream();
                        ObjectOutputStream oos = new ObjectOutputStream(baos);
                        oos.writeObject(body);
                        oos.close();
                        digest.update(baos.toByteArray());
                    } catch (IOException e) {
                        // Ignore error - this should not happen with Serializable objects
                    }
                } else {
                    // For other objects, convert to JSON bytes
                    // Note: This might not work for all request bodies, but it's the best we can do
                    try {
                        ByteArrayOutputStream baos = new ByteArrayOutputStream();
                        JsonGenerator generator = jsonpMapper.jsonProvider().createGenerator(baos, JsonEncoding.UTF8);
                        jsonpMapper.serialize(body, generator);
                        generator.close();
                        digest.update(baos.toByteArray());
                    } catch (IOException e) {
                        // Ignore error - if we can't serialize the body, we'll just not include it in the signature
                    }
                }
            }

            // Convert digest to hex string
            byte[] hash = digest.digest();
            StringBuilder hexString = new StringBuilder(2 * hash.length);
            for (byte b : hash) {
                String hex = Integer.toHexString(0xff & b);
                if (hex.length() == 1) {
                    hexString.append('0');
                }
                hexString.append(hex);
            }

            return hexString.toString();

        } catch (NoSuchAlgorithmException e) {
            // SHA-256 should be available in all Java implementations
            throw new RuntimeException("SHA-256 algorithm not available", e);
        }
    }

    private void updateDigest(MessageDigest digest, String value) {
        if (value != null) {
            digest.update(value.getBytes());
        }
    }
}