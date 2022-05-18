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

package org.apache.flink.table.gateway.rest;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.gateway.common.endpoint.SqlGatewayEndpoint;
import org.apache.flink.table.gateway.common.endpoint.SqlGatewayEndpointFactory;
import org.apache.flink.table.gateway.common.utils.SqlGatewayException;

import java.util.Collections;
import java.util.Set;

/** Factory to create the {@link SqlGatewayRestEndpoint}. */
public class SqlGatewayRestEndpointFactory implements SqlGatewayEndpointFactory {

    private static final String IDENTIFIER = "rest";

    @Override
    public SqlGatewayEndpoint createSqlGatewayEndpoint(Context context) {
        // TODO: fix this.
        Configuration configuration = new Configuration();
        try {
            return new SqlGatewayRestEndpoint(context.getSqlGatewayService(), configuration);
        } catch (Exception e) {
            throw new SqlGatewayException("Failed to create the rest endpoint.");
        }
    }

    @Override
    public String factoryIdentifier() {
        return IDENTIFIER;
    }

    @Override
    public Set<ConfigOption<?>> requiredOptions() {
        return Collections.emptySet();
    }

    @Override
    public Set<ConfigOption<?>> optionalOptions() {
        return Collections.emptySet();
    }
}
