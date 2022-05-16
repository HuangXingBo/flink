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
import org.apache.flink.table.gateway.common.utils.SqlGatewayException;
import org.apache.flink.table.gateway.service.utils.ThreadUtils;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.time.Duration;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;

import static org.apache.flink.core.testutils.FlinkAssertions.anyCauseMatches;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

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
    public void testOperationLifeCycle() {}

    @Test
    public void testCancelWhenOperationIsRunning() throws Exception {
        CountDownLatch isRunningLatch = new CountDownLatch(1);
        CountDownLatch operationIsInterrupted = new CountDownLatch(1);

        Operation op =
                new Operation(
                        OperationType.UNKNOWN,
                        handle -> {
                            isRunningLatch.countDown();
                            try {
                                Thread.sleep(10_000);
                            } catch (InterruptedException e) {
                                operationIsInterrupted.countDown();
                                throw new RuntimeException(e);
                            }
                            fail("Should fail.");
                            return null;
                        });
        OperationHandle handle = operationManager.submitOperation(op);
        isRunningLatch.countDown();
        // Make sure the Operation is sleeping
        Thread.sleep(1);
        operationManager.cancelOperation(handle);

        CommonTestUtils.waitUtil(
                () -> operationIsInterrupted.getCount() == 0,
                Duration.ofSeconds(10),
                "The inner invocation should be interrupted.");
        assertEquals(OperationStatus.CANCELED, op.getOperationStatus());
    }

    @Test
    public void testCancelWhenOperationMultiTimes() {
        CountDownLatch isRunningLatch = new CountDownLatch(1);
        CountDownLatch operationIsInterrupted = new CountDownLatch(1);

        Operation op =
                new Operation(
                        OperationType.UNKNOWN,
                        handle -> {
                            isRunningLatch.countDown();
                            try {
                                Thread.sleep(10_000);
                            } catch (InterruptedException e) {
                                operationIsInterrupted.countDown();
                                throw new RuntimeException(e);
                            }
                            // Should not reach here.
                            fail("Should fail");
                            return null;
                        });
        OperationHandle handle = operationManager.submitOperation(op);
        isRunningLatch.countDown();
        operationManager.cancelOperation(handle);

        assertThatThrownBy(() -> operationManager.cancelOperation(handle))
                .satisfies(
                        anyCauseMatches(
                                SqlGatewayException.class,
                                "Failed to convert the Operation Status from CANCELED to CANCELED."));
    }
}
