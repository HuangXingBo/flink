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

import org.apache.flink.table.gateway.common.utils.SqlGatewayException;
import org.apache.flink.table.gateway.service.result.ExecutionResult;
import org.apache.flink.table.planner.plan.metadata.FlinkDefaultRelMetadataProvider;
import org.apache.flink.util.TemporaryClassLoaderContext;

import org.apache.calcite.rel.metadata.JaninoRelMetadataProvider;
import org.apache.calcite.rel.metadata.RelMetadataQueryBase;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ExecutorService;
import java.util.function.Supplier;

/**
 * An implementation of {@link OperationExecutor} is delegator which wraps all methods under a given
 * {@link ClassLoader} and {@link ExecutorService} currently. In the future, it may wrap more
 * complex actions.
 */
public final class DelegateOperationExecutor implements OperationExecutor {

    private static final Logger LOG = LoggerFactory.getLogger(DelegateOperationExecutor.class);

    private final OperationExecutor delegator;
    private final ClassLoader classLoader;

    DelegateOperationExecutor(OperationExecutor delegator, ClassLoader classLoader) {
        this.delegator = delegator;
        this.classLoader = classLoader;
    }

    @Override
    public ExecutionResult executeStatement(String statement) {
        return wrapClassLoader(() -> delegator.executeStatement(statement));
    }

    /**
     * Executes the given supplier using the execution context's classloader as thread classloader.
     */
    private <R> R wrapClassLoader(Supplier<R> supplier) {
        try (TemporaryClassLoaderContext ignored = TemporaryClassLoaderContext.of(classLoader)) {
            // The creation thread of TableEnvironmentInternal and execution thread are not the
            // same, the threadlocal cache may have been cleaned up, so need reload the class.
            RelMetadataQueryBase.THREAD_PROVIDERS.set(
                    JaninoRelMetadataProvider.of(FlinkDefaultRelMetadataProvider.INSTANCE()));
            return supplier.get();
        } catch (Exception e) {
            throw new SqlGatewayException("Failed to execute operation", e);
        }
    }
}
