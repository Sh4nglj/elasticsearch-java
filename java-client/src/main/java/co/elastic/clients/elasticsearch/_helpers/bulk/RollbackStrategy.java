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

import co.elastic.clients.elasticsearch.core.BulkResponse;
import co.elastic.clients.elasticsearch.core.bulk.BulkOperation;
import co.elastic.clients.elasticsearch.core.bulk.BulkResponseItem;

import java.util.List;
import java.util.concurrent.CompletionStage;

/**
 * Custom rollback strategy interface for bulk operations.
 * Users can implement this interface to provide custom rollback logic.
 */
public interface RollbackStrategy<Context> {
    /**
     * Rollback the given operations.
     *
     * @param client the Elasticsearch client to use for rollback operations
     * @param operations the list of operations to rollback
     * @param contexts the contexts associated with the operations
     * @param response the bulk response containing the failure information
     * @return a completion stage that completes when the rollback is done
     */
    CompletionStage<Void> rollback(
            ElasticsearchAsyncClient client,
            List<BulkOperation> operations,
            List<Context> contexts,
            BulkResponse response
    );
}