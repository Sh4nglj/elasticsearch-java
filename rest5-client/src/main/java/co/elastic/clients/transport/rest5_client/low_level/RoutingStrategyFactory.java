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

/**
 * Factory for creating request routing strategies.
 */
public class RoutingStrategyFactory {
    
    /**
     * Routing strategy types.
     */
    public enum StrategyType {
        ROUND_ROBIN,
        WEIGHTED_ROUND_ROBIN,
        LEAST_CONNECTIONS,
        CONSISTENT_HASHING
    }
    
    /**
     * Creates a new routing strategy based on the given type.
     * @param type The routing strategy type
     * @return A new routing strategy instance
     */
    public static RequestRoutingStrategy createStrategy(StrategyType type) {
        switch (type) {
            case ROUND_ROBIN:
                return new RoundRobinRoutingStrategy();
            case WEIGHTED_ROUND_ROBIN:
                return new WeightedRoundRobinRoutingStrategy();
            case LEAST_CONNECTIONS:
                return new LeastConnectionsRoutingStrategy();
            case CONSISTENT_HASHING:
                return new ConsistentHashingRoutingStrategy();
            default:
                throw new IllegalArgumentException("Unknown routing strategy type: " + type);
        }
    }
    
    /**
     * Creates a new routing strategy based on the given type name.
     * @param typeName The routing strategy type name
     * @return A new routing strategy instance
     */
    public static RequestRoutingStrategy createStrategy(String typeName) {
        return createStrategy(StrategyType.valueOf(typeName.toUpperCase()));
    }
}