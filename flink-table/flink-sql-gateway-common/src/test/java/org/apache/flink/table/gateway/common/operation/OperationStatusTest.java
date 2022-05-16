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

package org.apache.flink.table.gateway.common.operation;

import org.junit.Test;

import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

/** Test {@link OperationStatus}. */
public class OperationStatusTest {

    @Test
    public void testAllTestSpec() {
        for (TestSpec spec : allTestSpec()) {
            assertEquals(
                    spec.isValid,
                    OperationStatus.isValidStatusTranslation(spec.fromStatus, spec.toStatus));
        }
    }

    private List<TestSpec> allTestSpec() {
        return Arrays.asList(
                TestSpec.newBuilder()
                        .from(OperationStatus.INITIALIZED)
                        .to(OperationStatus.PENDING)
                        .isValid(true)
                        .build(),
                TestSpec.newBuilder()
                        .from(OperationStatus.PENDING)
                        .to(OperationStatus.RUNNING)
                        .isValid(true)
                        .build(),
                TestSpec.newBuilder()
                        .from(OperationStatus.RUNNING)
                        .to(OperationStatus.FINISHED)
                        .isValid(true)
                        .build(),
                TestSpec.newBuilder()
                        .from(OperationStatus.INITIALIZED)
                        .to(OperationStatus.CANCELED)
                        .isValid(true)
                        .build(),
                TestSpec.newBuilder()
                        .from(OperationStatus.PENDING)
                        .to(OperationStatus.CANCELED)
                        .isValid(true)
                        .build(),
                TestSpec.newBuilder()
                        .from(OperationStatus.RUNNING)
                        .to(OperationStatus.CANCELED)
                        .isValid(true)
                        .build());
    }

    private static class TestSpec {

        final OperationStatus fromStatus;
        final OperationStatus toStatus;
        final boolean isValid;

        private TestSpec(OperationStatus fromStatus, OperationStatus toStatus, boolean isValid) {
            this.fromStatus = fromStatus;
            this.toStatus = toStatus;
            this.isValid = isValid;
        }

        static TestSpecBuilder newBuilder() {
            return new TestSpecBuilder();
        }

        private static class TestSpecBuilder {
            private OperationStatus fromStatus;
            private OperationStatus toStatus;
            private boolean isValid;

            TestSpecBuilder from(OperationStatus fromStatus) {
                this.fromStatus = fromStatus;
                return this;
            }

            TestSpecBuilder to(OperationStatus toStatus) {
                this.toStatus = toStatus;
                return this;
            }

            TestSpecBuilder isValid(boolean isValid) {
                this.isValid = isValid;
                return this;
            }

            TestSpec build() {
                return new TestSpec(fromStatus, toStatus, isValid);
            }
        }
    }
}
