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

package org.apache.flink.table.gateway.common;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.gateway.common.operation.OperationHandle;
import org.apache.flink.table.gateway.common.results.OperationInfo;
import org.apache.flink.table.gateway.common.results.ResultSet;
import org.apache.flink.table.gateway.common.session.SessionEnvironment;
import org.apache.flink.table.gateway.common.session.SessionHandle;
import org.apache.flink.table.gateway.common.utils.SqlGatewayException;

import java.util.Map;

/**
 * The definition for SQLGatewayService. The SQLGatewayService is the core to process the request
 * from the endpoints.
 */
public interface SQLGatewayService {

    // -------------------------------------------------------------------------------------------
    // Session Management
    // -------------------------------------------------------------------------------------------

    SessionHandle openSession(SessionEnvironment environment) throws SqlGatewayException;

    void closeSession(SessionHandle sessionHandle) throws SqlGatewayException;

    Map<String, String> getSessionConfig(SessionHandle sessionHandle) throws SqlGatewayException;

    // -------------------------------------------------------------------------------------------
    // Operation Management
    // -------------------------------------------------------------------------------------------

    void cancelOperation(SessionHandle sessionHandle, OperationHandle operationHandle)
            throws SqlGatewayException;

    void closeOperation(SessionHandle sessionHandle, OperationHandle operationHandle)
            throws SqlGatewayException;

    // -------------------------------------------------------------------------------------------
    // Statements
    // -------------------------------------------------------------------------------------------

    /**
     * Execute the statement with the specified Session. It allows to execute with Operation-level
     * configuration.
     */
    OperationHandle executeStatement(
            SessionHandle sessionHandle,
            String statement,
            long executionTimeoutMs,
            Configuration executionConfig)
            throws SqlGatewayException;

    /** Fetch the results with token id. */
    ResultSet fetchResults(
            SessionHandle sessionHandle, OperationHandle operationHandle, long token, int maxRows);

    OperationInfo getOperationInfo(SessionHandle sessionHandle, OperationHandle operationHandle);
}
