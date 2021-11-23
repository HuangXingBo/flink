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
package org.apache.beam.runners.fnexecution.environment;

import org.apache.beam.model.pipeline.v1.RunnerApi;
import org.apache.beam.model.pipeline.v1.RunnerApi.Environment;
import org.apache.beam.runners.core.construction.BeamUrns;
import org.apache.beam.runners.fnexecution.GrpcFnServer;
import org.apache.beam.runners.fnexecution.artifact.ArtifactRetrievalService;
import org.apache.beam.runners.fnexecution.control.ControlClientPool;
import org.apache.beam.runners.fnexecution.control.FnApiControlClientPoolService;
import org.apache.beam.runners.fnexecution.control.InstructionRequestHandler;
import org.apache.beam.runners.fnexecution.logging.GrpcLoggingService;
import org.apache.beam.runners.fnexecution.provisioning.StaticGrpcProvisionService;
import org.apache.beam.sdk.fn.IdGenerator;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.RemoteEnvironmentOptions;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.lang.reflect.Field;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Map;
import java.util.concurrent.TimeoutException;

/**
 * An {@link EnvironmentFactory} which forks processes based on the parameters in the Environment.
 * The returned {@link ProcessEnvironment} has to make sure to stop the processes.
 */
public class ProcessEnvironmentFactory implements EnvironmentFactory {

    private static final Logger LOG = LoggerFactory.getLogger(ProcessEnvironmentFactory.class);

    public static ProcessEnvironmentFactory create(
            ProcessManager processManager,
            GrpcFnServer<StaticGrpcProvisionService> provisioningServiceServer,
            ControlClientPool.Source clientSource,
            IdGenerator idGenerator,
            PipelineOptions pipelineOptions) {
        return new ProcessEnvironmentFactory(
                processManager,
                provisioningServiceServer,
                idGenerator,
                clientSource,
                pipelineOptions);
    }

    private final ProcessManager processManager;
    private final GrpcFnServer<StaticGrpcProvisionService> provisioningServiceServer;
    private final IdGenerator idGenerator;
    private final ControlClientPool.Source clientSource;
    private final PipelineOptions pipelineOptions;

    private ProcessEnvironmentFactory(
            ProcessManager processManager,
            GrpcFnServer<StaticGrpcProvisionService> provisioningServiceServer,
            IdGenerator idGenerator,
            ControlClientPool.Source clientSource,
            PipelineOptions pipelineOptions) {
        this.processManager = processManager;
        this.provisioningServiceServer = provisioningServiceServer;
        this.idGenerator = idGenerator;
        this.clientSource = clientSource;
        this.pipelineOptions = pipelineOptions;
    }

    /** Creates a new, active {@link RemoteEnvironment} backed by a forked process. */
    @Override
    public RemoteEnvironment createEnvironment(Environment environment, String workerId)
            throws Exception {
        Preconditions.checkState(
                environment
                        .getUrn()
                        .equals(
                                BeamUrns.getUrn(
                                        RunnerApi.StandardEnvironments.Environments.PROCESS)),
                "The passed environment does not contain a ProcessPayload.");
        final RunnerApi.ProcessPayload processPayload =
                RunnerApi.ProcessPayload.parseFrom(environment.getPayload());

        String executable = processPayload.getCommand();
        String provisionEndpoint = provisioningServiceServer.getApiServiceDescriptor().getUrl();

        String semiPersistDir =
                pipelineOptions.as(RemoteEnvironmentOptions.class).getSemiPersistDir();
        ImmutableList.Builder<String> argsBuilder =
                ImmutableList.<String>builder()
                        .add(String.format("--id=%s", workerId))
                        .add(String.format("--provision_endpoint=%s", provisionEndpoint));
        if (semiPersistDir != null) {
            argsBuilder.add(String.format("--semi_persist_dir=%s", semiPersistDir));
        }

        LOG.debug("Creating Process for worker ID {}", workerId);
        // Wrap the blocking call to clientSource.get in case an exception is thrown.
        InstructionRequestHandler instructionHandler = null;
        try {
            ProcessManager.RunningProcess process =
                    processManager.startProcess(
                            workerId, executable, argsBuilder.build(), processPayload.getEnvMap());
            // Wait on a client from the gRPC server.
            long pid = 0;
            while (instructionHandler == null) {
                try {
                    // If the process is not alive anymore, we abort.
                    pid = getPidOfProcess(process.getUnderlyingProcess());
                    process.isAliveOrThrow();
                    LOG.info("pid is {}", pid);
                    instructionHandler = clientSource.take(workerId, Duration.ofSeconds(5));
                } catch (TimeoutException timeoutEx) {
                    LOG.info(
                            "Still waiting for startup of environment '{}' for worker id {}",
                            processPayload.getCommand(),
                            workerId);
                    LOG.info(watchProcessState(pid));
                    LOG.info(getBootLog(processPayload.getEnvMap()));
                    LOG.info(watchProcessState("beam_boot.py"));
                    LOG.info(watchProcessState("beam_sdk_worker_main"));
                } catch (InterruptedException interruptEx) {
                    Thread.currentThread().interrupt();
                    throw new RuntimeException(interruptEx);
                }
            }
        } catch (Exception e) {
            try {
                processManager.stopProcess(workerId);
            } catch (Exception processKillException) {
                e.addSuppressed(processKillException);
            }
            throw e;
        }

        return ProcessEnvironment.create(processManager, environment, workerId, instructionHandler);
    }

    private String getBootLog(Map<String, String> envMap) throws IOException {
        String log = envMap.get("BOOT_LOG_DIR") + "/flink-python-udf-boot.log";
        File logFile = new File(log);
        StringBuilder output = new StringBuilder();
        BufferedReader br =
                new BufferedReader(
                        new InputStreamReader(
                                new FileInputStream(logFile), StandardCharsets.UTF_8));
        String line;
        output.append("\n");
        while ((line = br.readLine()) != null) {
            output.append(line).append("\n");
        }
        return output.toString();
    }

    private static synchronized long getPidOfProcess(Process p) {
        long pid = -1;

        try {
            if (p.getClass().getName().equals("java.lang.UNIXProcess")) {
                Field f = p.getClass().getDeclaredField("pid");
                f.setAccessible(true);
                pid = f.getLong(p);
                f.setAccessible(false);
            }
        } catch (Exception e) {
            pid = -1;
        }
        return pid;
    }

    private String watchProcessState(long pid) throws InterruptedException, IOException {
        if (pid == 0) {
            return "Wrong Pid";
        }
        String cmd = String.format("ps -ef | grep %s", pid);
        Process process = new ProcessBuilder("bash", "-c", cmd).redirectErrorStream(true).start();

        StringBuilder output = new StringBuilder();
        BufferedReader br = new BufferedReader(new InputStreamReader(process.getInputStream()));
        String line;
        output.append("\n");
        while ((line = br.readLine()) != null) {
            output.append(line).append("\n");
        }

        // There should really be a timeout here.
        if (0 != process.waitFor()) {
            return null;
        }

        return output.toString();
    }

    private String watchProcessState(String key) throws IOException, InterruptedException {
        String cmd = String.format("ps -ef | grep %s", key);
        Process process = new ProcessBuilder("bash", "-c", cmd).redirectErrorStream(true).start();

        StringBuilder output = new StringBuilder();
        BufferedReader br = new BufferedReader(new InputStreamReader(process.getInputStream()));
        String line;
        output.append("\n");
        while ((line = br.readLine()) != null) {
            output.append(line).append("\n");
        }

        // There should really be a timeout here.
        if (0 != process.waitFor()) {
            return null;
        }

        return output.toString();
    }

    /** Provider of ProcessEnvironmentFactory. */
    public static class Provider implements EnvironmentFactory.Provider {
        private final PipelineOptions pipelineOptions;

        public Provider(PipelineOptions options) {
            this.pipelineOptions = options;
        }

        @Override
        public EnvironmentFactory createEnvironmentFactory(
                GrpcFnServer<FnApiControlClientPoolService> controlServiceServer,
                GrpcFnServer<GrpcLoggingService> loggingServiceServer,
                GrpcFnServer<ArtifactRetrievalService> retrievalServiceServer,
                GrpcFnServer<StaticGrpcProvisionService> provisioningServiceServer,
                ControlClientPool clientPool,
                IdGenerator idGenerator) {
            return create(
                    ProcessManager.create(),
                    provisioningServiceServer,
                    clientPool.getSource(),
                    idGenerator,
                    pipelineOptions);
        }
    }
}
