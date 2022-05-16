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

package org.apache.flink.table.gateway.common.results;

import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.data.RowData;
import org.apache.flink.util.ExceptionUtils;

import com.sun.istack.internal.Nullable;

import java.util.List;
import java.util.stream.Collectors;

/** The collection of the results. */
public class ResultSet {

    private final ResultType resultType;

    private final Long nextToken;

    @Nullable private final ResolvedSchema resultSchema;
    @Nullable private final List<RowData> data;

    @Nullable private final Exception exception;

    public ResultSet(Exception e) {
        this(ResultType.ERROR, null, null, null, e);
    }

    public ResultSet(
            ResultType resultType,
            Long nextToken,
            ResolvedSchema resultSchema,
            List<RowData> results) {
        this(resultType, nextToken, resultSchema, results, null);
    }

    public ResultSet(
            ResultType resultType,
            Long nextToken,
            ResolvedSchema resultSchema,
            List<RowData> data,
            Exception e) {
        this.nextToken = nextToken;
        this.resultType = resultType;
        this.resultSchema = resultSchema;
        this.data = data;
        this.exception = e;
    }

    public ResultType getResultType() {
        return resultType;
    }

    public Long getNextToken() {
        return nextToken;
    }

    public ResolvedSchema getResultSchema() {
        return resultSchema;
    }

    public List<RowData> getData() {
        return data;
    }

    @Override
    public String toString() {
        return String.format(
                "ResultSet{\n"
                        + "  resultType=%s,\n"
                        + "  resultSchema=%s,\n"
                        + "  data=[%s],\n"
                        + "  exception=%s"
                        + "}",
                resultType,
                resultSchema == null ? "null" : resultSchema.toString(),
                data == null
                        ? "null"
                        : data.stream().map(Object::toString).collect(Collectors.joining(",")),
                exception != null ? ExceptionUtils.stringifyException(exception) : "null");
    }

    /** Describe the kind of the ResultSet. */
    public enum ResultType {
        PAYLOAD,

        EMPTY,

        EOS,

        ERROR
    }
}
