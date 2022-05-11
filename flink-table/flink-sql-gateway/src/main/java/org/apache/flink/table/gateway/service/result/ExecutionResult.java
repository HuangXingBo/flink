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

import org.apache.flink.core.execution.JobClient;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.ResultKind;
import org.apache.flink.table.api.internal.TableResultInternal;
import org.apache.flink.table.catalog.Column;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.gateway.common.utils.SqlGatewayException;

import com.sun.istack.internal.Nullable;

import java.util.Collections;
import java.util.List;

/** Describe the execution results. */
public class ExecutionResult {

    public static final ExecutionResult SUCCESS_EXECUTION_RESULT =
            new ExecutionResult(
                    ExecutionResultKind.SUCCESS,
                    ResolvedSchema.of(Column.physical("result", DataTypes.STRING())),
                    Collections.singletonList(GenericRowData.of(StringData.fromString("OK"))),
                    null,
                    null);

    public static ExecutionResult from(TableResultInternal tableResultInternal) {
        ResultKind kind = tableResultInternal.getResultKind();
        if (kind == ResultKind.SUCCESS) {
            return SUCCESS_EXECUTION_RESULT;
        } else if (kind == ResultKind.SUCCESS_WITH_CONTENT) {
            throw new UnsupportedOperationException("Not implemented yet.");
        } else {
            throw new IllegalArgumentException("Unknown result kind: " + kind);
        }
    }

    public static ExecutionResult from(SqlGatewayException e) {
        return new ExecutionResult(
                ExecutionResultKind.ERROR,
                ResolvedSchema.of(Column.physical("result", DataTypes.STRING())),
                Collections.singletonList(GenericRowData.of(StringData.fromString("Error"))),
                null,
                e);
    }

    private final ExecutionResultKind resultKind;

    private final ResolvedSchema resultSchema;

    private final List<RowData> results;

    private final @Nullable TableResultInternal tableResult;

    private final @Nullable SqlGatewayException exception;

    private ExecutionResult(
            ExecutionResultKind resultKind,
            ResolvedSchema resultSchema,
            List<RowData> results,
            @Nullable TableResultInternal resultInternal,
            @Nullable SqlGatewayException exception) {
        this.resultKind = resultKind;
        this.resultSchema = resultSchema;
        this.results = results;
        this.tableResult = resultInternal;
        this.exception = exception;
    }

    public ExecutionResultKind getResultKind() {
        return resultKind;
    }

    public ResolvedSchema getResolvedSchema() {
        return resultSchema;
    }

    public List<RowData> fetchResults(int token, int maxRows) {
        if (maxRows < 1) {
            throw new IllegalArgumentException("The max rows should be larger than 0.");
        }
        return results;
    }

    public SqlGatewayException getException() {
        return exception;
    }

    public void close() {
        if (tableResult != null) {
            tableResult.getJobClient().ifPresent(JobClient::cancel);
        }
    }
}
