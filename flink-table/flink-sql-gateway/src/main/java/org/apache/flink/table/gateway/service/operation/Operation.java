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

import org.apache.flink.table.gateway.common.operation.OperationStatus;
import org.apache.flink.table.gateway.common.operation.OperationType;
import org.apache.flink.table.gateway.common.results.ResultSet;
import org.apache.flink.table.gateway.common.utils.SqlGatewayException;
import org.apache.flink.table.gateway.service.result.ExecutionResult;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.function.RunnableWithException;
import org.apache.flink.util.function.SupplierWithException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Supplier;

/** The identity manges the execution, resources and execution results. */
public class Operation {

    private static final Logger LOG = LoggerFactory.getLogger(Operation.class);

    private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();

    private final OperationType operationType;
    private OperationStatus status;

    private final SupplierWithException<ExecutionResult, InterruptedException> resultSupplier;

    private Future<?> invocation;
    private ExecutionResult operationResult;

    public Operation(
            OperationType operationType,
            SupplierWithException<ExecutionResult, InterruptedException> resultSupplier) {
        this.status = OperationStatus.INITIALIZED;
        this.operationType = operationType;

        this.resultSupplier = resultSupplier;
    }

    private void runBefore() {
        updateState(OperationStatus.PENDING);
    }

    private void runAfter() {
        updateState(OperationStatus.FINISHED);
    }

    public void run(ExecutorService service) {
        invocation =
                service.submit(
                        () -> {
                            try {
                                runBefore();
                                updateState(OperationStatus.RUNNING);
                                ExecutionResult result = resultSupplier.get();
                                writeLock(() -> operationResult = result);
                                runAfter();
                            } catch (SqlGatewayException e) {
                                writeLock(
                                        () -> {
                                            updateState(OperationStatus.ERROR);
                                            operationResult =
                                                    ExecutionResult.from(
                                                            new SqlGatewayException(
                                                                    "Failed to execute the operation.",
                                                                    e));
                                        });
                                LOG.error("Failed to execute the operation.", e);
                            } catch (InterruptedException e) {
                                writeLock(
                                        () -> {
                                            operationResult =
                                                    ExecutionResult.from(
                                                            new SqlGatewayException(
                                                                    "The operation execution is interrupted.",
                                                                    e));
                                        });
                                LOG.error("The operation execution is interrupted.", e);
                            }
                        });
    }

    public void cancel() {
        writeLock(
                () -> {
                    updateState(OperationStatus.CANCELED);

                    if (invocation != null && !invocation.isDone()) {
                        invocation.cancel(true);
                    }

                    if (operationResult != null) {
                        operationResult.close();
                        operationResult = null;
                    }
                });
    }

    public void close() {
        writeLock(
                () -> {
                    updateState(OperationStatus.CLOSED);

                    if (invocation != null && !invocation.isDone()) {
                        invocation.cancel(true);
                    }

                    if (operationResult != null) {
                        operationResult.close();
                        operationResult = null;
                    }
                });
    }

    public ResultSet fetchResults(int token, int maxRows) {
        OperationStatus currentStatus = getOperationStatus();
        Preconditions.checkState(
                currentStatus == OperationStatus.ERROR || currentStatus == OperationStatus.FINISHED,
                String.format(
                        "Can not fetch results from the operation whose status is %s.",
                        currentStatus));
        if (currentStatus == OperationStatus.ERROR) {
            return new ResultSet(
                    ResultSet.ResultType.ERROR,
                    -1,
                    operationResult.getResolvedSchema(),
                    operationResult.fetchResults(token, maxRows),
                    operationResult.getException());
        } else {
            throw new UnsupportedOperationException("Not implemented yet.");
        }
    }

    public OperationStatus getOperationStatus() {
        return readLock(() -> status);
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

    private void writeLock(RunnableWithException runner) {
        lock.writeLock().lock();
        try {
            runner.run();
        } catch (Exception e) {
            throw new SqlGatewayException("Failed to execute operation.", e);
        } finally {
            lock.writeLock().unlock();
        }
    }
}
