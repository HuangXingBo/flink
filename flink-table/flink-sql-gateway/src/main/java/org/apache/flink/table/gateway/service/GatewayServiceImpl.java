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

package org.apache.flink.table.gateway.service;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.gateway.common.SQLGatewayService;
import org.apache.flink.table.gateway.common.operation.OperationHandle;
import org.apache.flink.table.gateway.common.results.FetchOrientation;
import org.apache.flink.table.gateway.common.results.ResultSet;
import org.apache.flink.table.gateway.common.session.SessionEnvironment;
import org.apache.flink.table.gateway.common.session.SessionHandle;
import org.apache.flink.table.gateway.common.utils.SqlGatewayException;
import org.apache.flink.table.gateway.service.session.SessionManager;

import java.util.Map;

/** The implementation for the {@link SQLGatewayService}. */
public class GatewayServiceImpl implements SQLGatewayService {

    private final SessionManager sessionManager;

    public GatewayServiceImpl(SessionManager sessionManager) {
        this.sessionManager = sessionManager;
    }

    @Override
    public SessionHandle openSession(SessionEnvironment environment) throws SqlGatewayException {
        return sessionManager.openSession(environment).getSessionHandle();
    }

    @Override
    public void closeSession(SessionHandle sessionHandle) throws SqlGatewayException {
        sessionManager.closeSession(sessionHandle);
    }

    @Override
    public Map<String, String> getSessionConfig(SessionHandle sessionHandle)
            throws SqlGatewayException {
        return sessionManager.getSession(sessionHandle).getSessionConfig();
    }

    @Override
    public void cancelOperation(SessionHandle sessionHandle, OperationHandle operationHandle)
            throws SqlGatewayException {
        throw new UnsupportedOperationException("Not implemented yet.");
    }

    @Override
    public void closeOperation(SessionHandle sessionHandle, OperationHandle operationHandle)
            throws SqlGatewayException {
        throw new UnsupportedOperationException("Not implemented yet.");
    }

    @Override
    public void configureSession(
            SessionHandle sessionHandle, String statement, long executionTimeoutMs)
            throws SqlGatewayException {
        throw new UnsupportedOperationException("Not implemented yet.");
    }

    @Override
    public OperationHandle executeStatement(
            SessionHandle sessionHandle,
            String statement,
            long executionTimeoutMs,
            Configuration executionConfig)
            throws SqlGatewayException {
        throw new UnsupportedOperationException("Not implemented yet.");
    }

    @Override
    public ResultSet fetchResults(
            SessionHandle sessionHandle, OperationHandle operationHandle, int token, int maxRows) {
        throw new UnsupportedOperationException("Not implemented yet.");
    }

    @Override
    public ResultSet fetchLog(
            SessionHandle sessionHandle,
            OperationHandle operationHandle,
            FetchOrientation orientation,
            int maxRows)
            throws SqlGatewayException {
        throw new UnsupportedOperationException("Not implemented yet.");
    }

    @Override
    public ResultSet fetchResult(
            SessionHandle sessionHandle,
            OperationHandle operationHandle,
            FetchOrientation orientation,
            int maxRows)
            throws SqlGatewayException {
        throw new UnsupportedOperationException("Not implemented yet.");
    }
}
