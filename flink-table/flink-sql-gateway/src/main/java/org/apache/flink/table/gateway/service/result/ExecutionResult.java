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

import org.apache.flink.table.api.ResultKind;
import org.apache.flink.table.api.internal.TableResultInternal;
import org.apache.flink.table.gateway.common.operation.OperationHandle;
import org.apache.flink.table.gateway.common.results.ResultSet;

/** Describe the execution results. */
public class ExecutionResult {

    public static ExecutionResult from(
            OperationHandle handle, TableResultInternal tableResultInternal) {
        ResultKind kind = tableResultInternal.getResultKind();
        if (kind == ResultKind.SUCCESS) {
            return new ExecutionResult(new ResultFetcher(handle, tableResultInternal, 1));
        } else if (kind == ResultKind.SUCCESS_WITH_CONTENT) {
            return new ExecutionResult(new ResultFetcher(handle, tableResultInternal, 5000));
        } else {
            throw new IllegalArgumentException();
        }
    }

    private final ResultFetcher resultFetcher;

    private ExecutionResult(ResultFetcher fetcher) {
        this.resultFetcher = fetcher;
    }

    public ResultSet fetchResults(long token, int maxRows) {
        if (maxRows < 1) {
            throw new IllegalArgumentException("The max rows should be larger than 0.");
        }
        return resultFetcher.fetchResult(token, maxRows);
    }

    public void close() {
        resultFetcher.close();
    }
}
