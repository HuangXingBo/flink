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

package org.apache.flink.table.gateway.common.session;

import org.apache.flink.table.gateway.common.endpoint.EndpointVersion;
import org.apache.flink.util.Preconditions;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/** Environment to initialize the {@code Session}. */
public class SessionEnvironment {
    private final @Nullable String sessionName;
    private final EndpointVersion version;
    private final List<String> libs;
    private final List<String> jars;
    private final Map<String, String> sessionConfig;

    private SessionEnvironment(
            @Nullable String sessionName,
            EndpointVersion version,
            List<String> libs,
            List<String> jars,
            Map<String, String> sessionConfig) {
        this.sessionName = sessionName;
        this.version = version;
        this.libs = libs;
        this.jars = jars;
        this.sessionConfig = sessionConfig;
    }

    // -------------------------------------------------------------------------------------------
    // Getter
    // -------------------------------------------------------------------------------------------

    public Optional<String> getSessionName() {
        return Optional.ofNullable(sessionName);
    }

    public EndpointVersion getSessionEndpointVersion() {
        return version;
    }

    public List<String> getJars() {
        return Collections.unmodifiableList(jars);
    }

    public List<String> getLibs() {
        return Collections.unmodifiableList(libs);
    }

    public Map<String, String> getSessionConfig() {
        return Collections.unmodifiableMap(sessionConfig);
    }

    // -------------------------------------------------------------------------------------------
    // Builder
    // -------------------------------------------------------------------------------------------

    public static Builder newBuilder() {
        return new Builder();
    }

    /** Builder to build the {@link SessionEnvironment}. */
    public static class Builder {
        private @Nullable String sessionName;
        private EndpointVersion version;
        private final List<String> libs = new ArrayList<>();
        private final List<String> jars = new ArrayList<>();
        private final Map<String, String> sessionConfig = new HashMap<>();

        public Builder setSessionName(String sessionName) {
            this.sessionName = sessionName;
            return this;
        }

        public Builder setSessionEndpointVersion(EndpointVersion version) {
            this.version = version;
            return this;
        }

        public Builder addLibs(String... libs) {
            this.libs.addAll(Arrays.asList(libs));
            return this;
        }

        public Builder addJars(String... jars) {
            this.jars.addAll(Arrays.asList(jars));
            return this;
        }

        public Builder addSessionConfig(Map<String, String> sessionConfig) {
            this.sessionConfig.putAll(sessionConfig);
            return this;
        }

        public SessionEnvironment build() {
            return new SessionEnvironment(
                    sessionName, Preconditions.checkNotNull(version), libs, jars, sessionConfig);
        }
    }
}
