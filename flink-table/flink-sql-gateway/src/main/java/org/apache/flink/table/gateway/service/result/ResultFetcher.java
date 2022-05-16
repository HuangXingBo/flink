/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.table.gateway.service.result;

import org.apache.flink.table.api.internal.TableResultInternal;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.gateway.common.operation.OperationHandle;
import org.apache.flink.table.gateway.common.results.ResultSet;
import org.apache.flink.table.gateway.service.utils.SqlExecutionException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Optional;

/** A fetcher to fetch result from submitted preview job. */
public class ResultFetcher {

    private static final Logger LOG = LoggerFactory.getLogger(ResultFetcher.class);

    private final OperationHandle operationHandle;

    private final ResolvedSchema resultSchema;
    private final ResultStore resultStore;
    private final LinkedList<RowData> bufferedResults = new LinkedList<>();

    private long currentToken = 0;
    private int previousMaxFetchSize = 0;
    private int previousResultSetSize = 0;
    private boolean noMoreResults = false;

    public ResultFetcher(
            OperationHandle operationHandle, TableResultInternal tableResult, int maxBufferSize) {
        this.operationHandle = operationHandle;
        this.resultSchema = tableResult.getResolvedSchema();
        this.resultStore = new ResultStore(tableResult, maxBufferSize);
    }

    public void close() {
        resultStore.close();
    }

    public ResultSet fetchResult(long token, int maxFetchSize) {
        if (token == currentToken) {
            if (noMoreResults) {
                if (LOG.isDebugEnabled()) {
                    LOG.debug("There is no more result for operation: {}.", operationHandle);
                }
                return new ResultSet(
                        ResultSet.ResultType.EOS, null, resultSchema, Collections.emptyList());
            }

            // a new token arrives, remove used results
            for (int i = 0; i < previousResultSetSize; i++) {
                bufferedResults.removeFirst();
            }

            if (bufferedResults.isEmpty()) {
                // buffered results have been totally consumed,
                // so try to fetch new results
                Optional<List<RowData>> newResults = resultStore.retrieveRecords();
                if (newResults.isPresent()) {
                    bufferedResults.addAll(newResults.get());
                    currentToken++;
                } else {
                    noMoreResults = true;
                    return new ResultSet(
                            ResultSet.ResultType.EOS, null, resultSchema, Collections.emptyList());
                }
            } else {
                // buffered results haven't been totally consumed
                currentToken++;
            }

            previousMaxFetchSize = maxFetchSize;
            if (maxFetchSize > 0) {
                previousResultSetSize = Math.min(bufferedResults.size(), maxFetchSize);
            } else {
                previousResultSetSize = bufferedResults.size();
            }
            if (LOG.isDebugEnabled()) {
                LOG.debug(
                        "Fetching current result for operation: {}, token: {}, maxFetchSize: {}, realReturnSize: {}.",
                        operationHandle,
                        token,
                        maxFetchSize,
                        previousResultSetSize);
            }
        } else if (token == currentToken - 1 && token >= 0) {
            // TODO: sync token >= 0 ?
            if (LOG.isDebugEnabled()) {
                LOG.debug(
                        "Fetching previous result for operation: {}, token: {}, maxFetchSize: {}",
                        operationHandle,
                        token,
                        maxFetchSize);
            }
            if (previousMaxFetchSize != maxFetchSize) {
                String msg =
                        String.format(
                                "As the same token is provided, fetch size must be the same. Expecting max_fetch_size to be %s.",
                                previousMaxFetchSize);
                if (LOG.isDebugEnabled()) {
                    LOG.error(msg);
                }
                return new ResultSet(new SqlExecutionException(msg));
            }
        } else {
            String msg;
            if (currentToken == 0) {
                msg = "Expecting token to be 0, but found " + token + ".";
            } else {
                msg =
                        "Expecting token to be "
                                + currentToken
                                + " or "
                                + (currentToken - 1)
                                + ", but found "
                                + token
                                + ".";
            }
            if (LOG.isDebugEnabled()) {
                LOG.error(msg);
            }
            return new ResultSet(new SqlExecutionException(msg));
        }

        return new ResultSet(
                ResultSet.ResultType.PAYLOAD,
                currentToken,
                resultSchema,
                getLinkedListElementsFromBegin(bufferedResults, previousResultSetSize));
    }

    private static <T> List<T> getLinkedListElementsFromBegin(LinkedList<T> linkedList, int size) {
        if (linkedList == null) {
            return null;
        }
        List<T> ret = new ArrayList<>();
        Iterator<T> iter = linkedList.iterator();
        for (int i = 0; i < size; i++) {
            ret.add(iter.next());
        }
        return ret;
    }
}
