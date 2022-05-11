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

import org.apache.flink.table.gateway.common.operation.OperationHandle;
import org.apache.flink.table.gateway.common.utils.SqlGatewayException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutorService;

/** Manage the lifecycle of the {@code Operation}. */
public class OperationManager {

    private static final Logger LOG = LoggerFactory.getLogger(OperationManager.class);

    private final Map<OperationHandle, Operation> submittedOperations;
    private final ExecutorService service;

    public OperationManager(ExecutorService service) {
        this.service = service;
        submittedOperations = new HashMap<>();
    }

    public OperationHandle submitOperation(Operation operation) {
        OperationHandle handle = OperationHandle.create();
        submittedOperations.put(handle, operation);
        operation.run(service);
        return handle;
    }

    public void cancelOperation(OperationHandle operationHandle) {
        checkOperationExists(operationHandle);
        submittedOperations.get(operationHandle).cancel();
    }

    public void closeOperation(OperationHandle operationHandle) {
        checkOperationExists(operationHandle);
        submittedOperations.remove(operationHandle).close();
    }

    public void close() {
        LOG.info("Close the Operation Manager.");
        for (Operation registeredOp : submittedOperations.values()) {
            registeredOp.close();
        }
    }

    private void checkOperationExists(OperationHandle operationHandle) {
        if (!submittedOperations.containsKey(operationHandle)) {
            throw new SqlGatewayException(
                    String.format(
                            "Can not find the operation in the OperationManager with the OperationHandle: %s.",
                            operationHandle));
        }
    }
}
