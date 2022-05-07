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

package org.apache.flink.table.gateway.common.results;

import org.apache.flink.table.gateway.common.operation.OperationStatus;
import org.apache.flink.table.gateway.common.operation.OperationType;

/** Info that describe the {@code Operation}. */
public class OperationInfo {

    private final OperationStatus status;

    private final OperationType type;

    private final boolean hasResults;

    public OperationInfo(OperationStatus status, OperationType type, boolean hasResults) {
        this.status = status;
        this.type = type;
        this.hasResults = hasResults;
    }

    public OperationStatus getStatus() {
        return status;
    }

    public OperationType getType() {
        return type;
    }

    public boolean isHasResults() {
        return hasResults;
    }
}
