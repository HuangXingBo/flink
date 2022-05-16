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
import org.apache.flink.table.gateway.common.operation.OperationStatus;
import org.apache.flink.table.gateway.common.operation.OperationType;
import org.apache.flink.table.gateway.common.results.OperationInfo;
import org.apache.flink.table.gateway.common.results.ResultSet;
import org.apache.flink.table.gateway.common.utils.SqlGatewayException;
import org.apache.flink.table.gateway.service.result.ExecutionResult;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Function;
import java.util.function.Supplier;

/** The identity manges the execution, resources and execution results. */
public class Operation {

    private static final Logger LOG = LoggerFactory.getLogger(Operation.class);

    private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();

    private final OperationType operationType;
    private OperationStatus status;

    private final Function<OperationHandle, ExecutionResult> resultSupplier;

    private Future<?> invocation;
    private ExecutionResult operationResult;
    private Exception operationError;

    public Operation(
            OperationType operationType,
            Function<OperationHandle, ExecutionResult> resultSupplier) {
        // TODO: add compile statement
        this.status = OperationStatus.INITIALIZED;
        this.operationType = operationType;

        this.resultSupplier = resultSupplier;
    }

    void runBefore() {
        updateState(OperationStatus.PENDING);
    }

    void runAfter() {
        updateState(OperationStatus.FINISHED);
    }

    public void run(OperationHandle handle, ExecutorService service) {
        invocation =
                service.submit(
                        () -> {
                            try {
                                runBefore();
                                updateState(OperationStatus.RUNNING);
                                ExecutionResult result = resultSupplier.apply(handle);
                                writeLock(() -> operationResult = result);
                                runAfter();
                            } catch (Exception e) {
                                LOG.error("Failed to execute the operation.", e);
                                writeLock(
                                        () -> {
                                            updateState(OperationStatus.ERROR);
                                            operationError = e;
                                        });
                            }
                        });
    }

    public void cancel() {
        writeLock(
                () -> {
                    if (invocation != null && !invocation.isDone()) {
                        invocation.cancel(true);
                    }

                    if (operationResult != null) {
                        operationResult.close();
                        operationResult = null;
                    }

                    updateState(OperationStatus.CANCELED);
                });
    }

    public void close() {
        writeLock(
                () -> {
                    if (invocation != null && !invocation.isDone()) {
                        invocation.cancel(true);
                    }

                    if (operationResult != null) {
                        operationResult.close();
                        operationResult = null;
                    }

                    updateState(OperationStatus.CLOSED);
                });
    }

    public ResultSet fetchResults(long token, int maxRows) {
        OperationStatus currentStatus = getOperationStatus();

        if (currentStatus == OperationStatus.ERROR) {
            return new ResultSet(operationError);
        } else if (currentStatus == OperationStatus.FINISHED) {
            return operationResult.fetchResults(token, maxRows);
        } else {
            throw new SqlGatewayException(
                    String.format("Can not fetch results in status %s.", currentStatus));
        }
    }

    public OperationStatus getOperationStatus() {
        return readLock(() -> status);
    }

    public OperationInfo getOperationInfo() {
        return readLock(() -> new OperationInfo(status, operationType, true));
    }

    private void updateState(OperationStatus toStatus) {
        writeLock(
                () -> {
                    boolean isValid = OperationStatus.isValidStatusTranslation(status, toStatus);
                    if (!isValid) {
                        String message =
                                String.format(
                                        "Failed to convert the Operation Status from %s to %s.",
                                        status, toStatus);
                        LOG.info(message);
                        throw new SqlGatewayException(message);
                    }
                    status = toStatus;
                });
    }

    // ------------------------------------------------------------------------------------------
    // Locks
    // ------------------------------------------------------------------------------------------

    private <T> T readLock(Supplier<T> supplier) {
        lock.readLock().lock();
        try {
            return supplier.get();
        } finally {
            lock.readLock().unlock();
        }
    }

    private void writeLock(Runnable runner) {
        lock.writeLock().lock();
        try {
            runner.run();
        } finally {
            lock.writeLock().unlock();
        }
    }
}
