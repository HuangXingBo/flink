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

import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.rest.RestServerEndpoint;
import org.apache.flink.runtime.rest.handler.RestHandlerSpecification;
import org.apache.flink.table.gateway.common.SqlGatewayService;
import org.apache.flink.table.gateway.common.endpoint.SqlGatewayEndpoint;
import org.apache.flink.table.gateway.rest.handler.CloseSessionHandler;
import org.apache.flink.table.gateway.rest.handler.CloseSessionHeaders;
import org.apache.flink.table.gateway.rest.handler.OpenSessionHandler;
import org.apache.flink.table.gateway.rest.handler.OpenSessionHeaders;
import org.apache.flink.util.ConfigurationException;

import org.apache.flink.shaded.netty4.io.netty.channel.ChannelInboundHandler;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;

/** Rest endpoint for the {@link SqlGatewayEndpoint}. */
public class SqlGatewayRestEndpoint extends RestServerEndpoint implements SqlGatewayEndpoint {

    private final SqlGatewayService service;

    public SqlGatewayRestEndpoint(SqlGatewayService service, Configuration configuration)
            throws IOException, ConfigurationException {
        super(configuration);
        this.service = service;
    }

    @Override
    protected List<Tuple2<RestHandlerSpecification, ChannelInboundHandler>> initializeHandlers(
            CompletableFuture<String> localAddressFuture) {
        Time timeout = Time.seconds(1);
        List<Tuple2<RestHandlerSpecification, ChannelInboundHandler>> handlers =
                new ArrayList<>(32);

        // Open a session
        OpenSessionHandler openSessionHandler =
                new OpenSessionHandler(
                        service, timeout, responseHeaders, OpenSessionHeaders.getInstance());
        handlers.add(Tuple2.of(OpenSessionHeaders.getInstance(), openSessionHandler));

        // Close a session
        CloseSessionHandler closeSessionHandler =
                new CloseSessionHandler(
                        service, timeout, responseHeaders, CloseSessionHeaders.getInstance());
        handlers.add(Tuple2.of(CloseSessionHeaders.getInstance(), closeSessionHandler));

        return handlers;
    }

    @Override
    protected void startInternal() throws Exception {
        // do nothing
    }

    @Override
    public void stop() throws Exception {
        close();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof SqlGatewayRestEndpoint)) {
            return false;
        }
        SqlGatewayRestEndpoint that = (SqlGatewayRestEndpoint) o;
        return Objects.equals(service, that.service);
    }

    @Override
    public int hashCode() {
        return Objects.hash(service);
    }
}
