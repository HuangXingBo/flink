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

package org.apache.flink.table.gateway.service.operation;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.table.api.internal.TableEnvironmentInternal;
import org.apache.flink.table.gateway.service.context.SessionContext;
import org.apache.flink.table.gateway.service.result.ExecutionResult;
import org.apache.flink.table.operations.BeginStatementSetOperation;
import org.apache.flink.table.operations.EndStatementSetOperation;
import org.apache.flink.table.operations.Operation;
import org.apache.flink.table.operations.command.AddJarOperation;
import org.apache.flink.table.operations.command.RemoveJarOperation;
import org.apache.flink.table.operations.command.ResetOperation;
import org.apache.flink.table.operations.command.SetOperation;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/** An implementation of {@link OperationExecutor}. */
public final class OperationExecutorImpl implements OperationExecutor {

    private static final Logger LOG = LoggerFactory.getLogger(OperationExecutorImpl.class);

    private final SessionContext sessionContext;

    @VisibleForTesting
    public OperationExecutorImpl(SessionContext context) {
        this.sessionContext = context;
    }

    @Override
    public ExecutionResult executeStatement(String statement) {
        TableEnvironmentInternal tableEnv = sessionContext.createTableEnvironment();

        List<Operation> parsedOperations = tableEnv.getParser().parse(statement);
        if (parsedOperations.size() > 1) {
            throw new UnsupportedOperationException();
        }
        Operation op = parsedOperations.get(0);
        if (op instanceof SetOperation || op instanceof ResetOperation) {
            throw new UnsupportedOperationException();
        } else if (op instanceof AddJarOperation || op instanceof RemoveJarOperation) {
            throw new UnsupportedOperationException();
        } else if (op instanceof BeginStatementSetOperation) {
            throw new UnsupportedOperationException();
        } else if (op instanceof EndStatementSetOperation) {
            throw new UnsupportedOperationException();
        } else {
            return ExecutionResult.from(tableEnv.executeInternal(op));
        }
    }
}
