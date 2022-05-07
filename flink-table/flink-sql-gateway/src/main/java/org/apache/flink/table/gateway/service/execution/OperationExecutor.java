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

package org.apache.flink.table.gateway.service.execution;

import org.apache.flink.runtime.security.contexts.SecurityContext;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.internal.TableEnvironmentInternal;
import org.apache.flink.table.gateway.common.utils.SqlGatewayException;
import org.apache.flink.table.gateway.service.result.ExecutionResult;

import java.util.concurrent.Future;

/** The executor to communicate with Flink {@link TableEnvironment}. */
public interface OperationExecutor {

    /** Creates a new instance of {@link OperationExecutor}. */
    static OperationExecutor createExecutor(
            TableEnvironmentInternal tEnv, ClassLoader classLoader, SecurityContext context) {
        return new DelegateOperationExecutor(new OperationExecutorImpl(tEnv), classLoader, context);
    }

    default ExecutionResult executeStatementSync(String statement) {
        try {
            return executeStatement(statement).get();
        } catch (Exception e) {
            // ignore
            throw new SqlGatewayException(e);
        }
    }

    Future<ExecutionResult> executeStatement(String statement);
}
