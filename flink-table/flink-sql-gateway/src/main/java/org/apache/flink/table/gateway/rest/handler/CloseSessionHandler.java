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

package org.apache.flink.table.gateway.rest.handler;

import org.apache.flink.api.common.time.Time;
import org.apache.flink.runtime.rest.handler.HandlerRequest;
import org.apache.flink.runtime.rest.handler.RestHandlerException;
import org.apache.flink.runtime.rest.messages.EmptyRequestBody;
import org.apache.flink.runtime.rest.messages.MessageHeaders;
import org.apache.flink.table.gateway.common.SqlGatewayService;
import org.apache.flink.table.gateway.common.session.SessionHandle;
import org.apache.flink.table.gateway.common.utils.SqlGatewayException;
import org.apache.flink.table.gateway.rest.RestEndpointVersion;
import org.apache.flink.table.gateway.rest.message.CloseSessionResponseBody;
import org.apache.flink.table.gateway.rest.message.SessionHandlePathParameter;
import org.apache.flink.table.gateway.rest.message.SessionMessageParameters;

import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.HttpResponseStatus;

import javax.annotation.Nonnull;

import java.util.Map;
import java.util.concurrent.CompletableFuture;

/** Handler to close the Session. */
public class CloseSessionHandler
        extends AbstractSqlGatewayRestHandler<
                EmptyRequestBody, CloseSessionResponseBody, SessionMessageParameters> {

    public CloseSessionHandler(
            SqlGatewayService service,
            Time timeout,
            Map<String, String> responseHeaders,
            MessageHeaders<EmptyRequestBody, CloseSessionResponseBody, SessionMessageParameters>
                    messageHeaders) {
        super(service, timeout, responseHeaders, messageHeaders);
    }

    @Override
    protected CompletableFuture<CloseSessionResponseBody> handleRequest(
            RestEndpointVersion version, @Nonnull HandlerRequest<EmptyRequestBody> request)
            throws RestHandlerException {
        SessionHandle sessionHandle = request.getPathParameter(SessionHandlePathParameter.class);
        try {
            service.closeSession(sessionHandle);
            return CompletableFuture.completedFuture(new CloseSessionResponseBody("CLOSED"));
        } catch (SqlGatewayException e) {
            throw new RestHandlerException(
                    e.getMessage(), HttpResponseStatus.INTERNAL_SERVER_ERROR, e);
        }
    }
}
