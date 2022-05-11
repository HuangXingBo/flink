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

import org.apache.flink.core.testutils.CommonTestUtils;
import org.apache.flink.table.gateway.common.operation.OperationHandle;
import org.apache.flink.table.gateway.common.operation.OperationStatus;
import org.apache.flink.table.gateway.common.operation.OperationType;
import org.apache.flink.table.gateway.service.utils.ThreadUtils;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.time.Duration;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;

import static org.apache.flink.table.gateway.service.result.ExecutionResult.SUCCESS_EXECUTION_RESULT;
import static org.junit.Assert.assertEquals;

/** Test for {@link OperationManager}. */
public class OperationManagerTest {

    private static ExecutorService service;
    private static OperationManager operationManager;

    @BeforeClass
    public static void setup() {
        service = ThreadUtils.newDaemonQueuedThreadPool(5, 500, 60_000, "test-service");
        operationManager = new OperationManager(service);
    }

    @AfterClass
    public static void cleanup() {
        if (operationManager != null) {
            operationManager.close();
        }

        if (service != null) {
            service.shutdown();
        }
    }

    @Test
    public void testCancelWhenOperationIsRunning() throws Exception {
        CountDownLatch isRunningLatch = new CountDownLatch(1);
        CountDownLatch operationIsInterrupted = new CountDownLatch(1);

        Operation op =
                new Operation(
                        OperationType.UNKNOWN,
                        () -> {
                            isRunningLatch.countDown();
                            try {
                                Thread.sleep(10_000);
                            } catch (InterruptedException e) {
                                operationIsInterrupted.countDown();
                                throw e;
                            }
                            // Should not reach here.
                            return SUCCESS_EXECUTION_RESULT;
                        });
        OperationHandle handle = operationManager.submitOperation(op);
        isRunningLatch.countDown();
        operationManager.cancelOperation(handle);

        CommonTestUtils.waitUtil(
                () -> operationIsInterrupted.getCount() == 0,
                Duration.ofSeconds(5),
                "The inner invocation should be interrupted.");
        assertEquals(OperationStatus.CANCELED, op.getOperationStatus());
    }

    @Test
    public void testCancelWhenOperationIsFinished() throws Exception {
        Operation op =
                new Operation(
                        OperationType.UNKNOWN,
                        () -> {
                            return SUCCESS_EXECUTION_RESULT;
                        });

        op.cancel();
        operationManager.submitOperation(op);

        assertEquals(OperationStatus.CANCELED, op.getOperationStatus());
    }
}
