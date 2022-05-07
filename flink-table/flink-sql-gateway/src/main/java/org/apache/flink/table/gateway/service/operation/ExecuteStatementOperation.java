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

import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.gateway.common.operation.OperationStatus;
import org.apache.flink.table.gateway.common.results.OperationInfo;
import org.apache.flink.table.gateway.service.execution.OperationExecutor;
import org.apache.flink.table.gateway.service.result.ExecutionResult;
import org.apache.flink.table.gateway.service.result.ExecutionResultKind;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Supplier;

import static org.apache.flink.table.gateway.common.operation.OperationType.EXECUTE_STATEMENT;

/** The operation to execute the query. */
public class ExecuteStatementOperation implements Operation {

    private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();

    private final String statement;
    private OperationStatus status;

    private final ExecutorService service;
    private final OperationExecutor executor;

    private Future<ExecutionResult> resultFuture;
    private ExecutionResult result;

    public ExecuteStatementOperation(
            ExecutorService service, OperationExecutor executor, String statement) {
        this.statement = statement;
        this.status = OperationStatus.INITIALIZED;

        this.executor = executor;
        this.service = service;
    }

    protected void runBefore() {
        updateState(OperationStatus.PENDING);
    }

    protected void runAfter() {
        updateState(OperationStatus.FINISHED);
    }

    @Override
    public void run() {
        Future<ExecutionResult> submit =
                service.submit(
                        () -> {
                            runBefore();
                            ExecutionResult result = executor.executeStatementSync(statement);
                            runAfter();
                            return result;
                        });
        writeLock(() -> resultFuture = submit);
        // register the timer to cancel the future if it is timeout
    }

    @Override
    public OperationInfo getOperationInfo() {
        // should copy the value
        return new OperationInfo(status, EXECUTE_STATEMENT, true);
    }

    @Override
    public void cancel() {
        writeLock(
                () -> {
                    if (resultFuture != null && !resultFuture.isDone()) {
                        resultFuture.cancel(true);
                    }

                    if (result != null) {
                        result.close();
                        result = null;
                    }

                    updateState(OperationStatus.CANCELED);
                });
    }

    @Override
    public void close() {
        if (!resultFuture.isDone()) {
            resultFuture.cancel(true);
        }

        if (result != null) {
            result.close();
            result = null;
        }
        updateState(OperationStatus.CLOSED);
    }

    @Override
    public ResolvedSchema getResultSchema() {
        if (!resultFuture.isDone()) {
            throw new IllegalArgumentException("The operation status is not ready.");
        }

        if (result.getResultKind() == ExecutionResultKind.ERROR) {
            throw new IllegalArgumentException("Can not get status for the error.");
        }

        return result.getResolvedSchema();
    }

    @Override
    public OperationStatus getOperationStatus() {
        return readLock(() -> status);
    }

    private boolean updateState(OperationStatus toStatus) {
        return writeLock(
                () -> {
                    boolean isValid = OperationStatus.isValidStatusTranslation(status, toStatus);
                    if (!isValid) {
                        return false;
                    }
                    status = toStatus;
                    return true;
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

    private <T> T writeLock(Supplier<T> supplier) {
        lock.writeLock().lock();
        try {
            return supplier.get();
        } finally {
            lock.writeLock().unlock();
        }
    }
}
