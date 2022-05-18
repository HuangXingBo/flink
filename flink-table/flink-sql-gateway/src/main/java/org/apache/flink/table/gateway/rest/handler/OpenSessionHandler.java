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
import org.apache.flink.runtime.rest.messages.EmptyMessageParameters;
import org.apache.flink.runtime.rest.messages.MessageHeaders;
import org.apache.flink.table.gateway.common.SqlGatewayService;
import org.apache.flink.table.gateway.common.session.SessionEnvironment;
import org.apache.flink.table.gateway.common.session.SessionHandle;
import org.apache.flink.table.gateway.rest.RestEndpointVersion;
import org.apache.flink.table.gateway.rest.message.OpenSessionRequestBody;
import org.apache.flink.table.gateway.rest.message.OpenSessionResponseBody;

import javax.annotation.Nonnull;

import java.util.Map;
import java.util.concurrent.CompletableFuture;

/** Open session Handler. */
public class OpenSessionHandler
        extends AbstractSqlGatewayRestHandler<
                OpenSessionRequestBody, OpenSessionResponseBody, EmptyMessageParameters> {

    public OpenSessionHandler(
            SqlGatewayService service,
            Time timeout,
            Map<String, String> responseHeaders,
            MessageHeaders<OpenSessionRequestBody, OpenSessionResponseBody, EmptyMessageParameters>
                    messageHeaders) {
        super(service, timeout, responseHeaders, messageHeaders);
    }

    @Override
    protected CompletableFuture<OpenSessionResponseBody> handleRequest(
            RestEndpointVersion version, @Nonnull HandlerRequest<OpenSessionRequestBody> request)
            throws RestHandlerException {
        String sessionName = request.getRequestBody().getSessionName();
        SessionHandle sessionHandle =
                service.openSession(
                        SessionEnvironment.newBuilder()
                                .setSessionEndpointVersion(RestEndpointVersion.V1)
                                .setSessionName(sessionName)
                                .addLibs(request.getRequestBody().getLibs().toArray(new String[0]))
                                .addJars(request.getRequestBody().getJars().toArray(new String[0]))
                                .build());

        return CompletableFuture.completedFuture(new OpenSessionResponseBody(sessionHandle));
    }
}
