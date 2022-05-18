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

package org.apache.flink.table.gateway.rest.message;

import org.apache.flink.runtime.rest.messages.ConversionException;
import org.apache.flink.runtime.rest.messages.MessagePathParameter;
import org.apache.flink.table.gateway.common.session.SessionHandle;
import org.apache.flink.table.gateway.common.utils.SqlGatewayException;
import org.apache.flink.util.InstantiationUtil;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

/** {@link MessagePathParameter} that parse the {@link SessionHandle}. */
public class SessionHandlePathParameter extends MessagePathParameter<SessionHandle> {

    public static final String KEY = "session_handle";

    public SessionHandlePathParameter() {
        super(KEY);
    }

    @Override
    protected SessionHandle convertFromString(String value) throws ConversionException {
        try {
            return InstantiationUtil.deserializeObject(
                    value.getBytes(StandardCharsets.UTF_8),
                    Thread.currentThread().getContextClassLoader());
        } catch (Exception e) {
            throw new ConversionException(
                    String.format("Failed to convert %s to the SessionHandle.", value), e);
        }
    }

    @Override
    protected String convertToString(SessionHandle sessionHandle) {
        try {
            return new String(
                    InstantiationUtil.serializeObject(sessionHandle), StandardCharsets.UTF_8);
        } catch (IOException e) {
            throw new SqlGatewayException("Should happen. Please fill an issue and report.", e);
        }
    }

    @Override
    public String getDescription() {
        return "The SessionHandle that identifies a session.";
    }
}
