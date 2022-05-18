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

package org.apache.flink.table.gateway.service;

import org.apache.flink.client.cli.DefaultCLI;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.api.config.ExecutionConfigOptions;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.gateway.common.SqlGatewayService;
import org.apache.flink.table.gateway.common.operation.OperationHandle;
import org.apache.flink.table.gateway.common.results.ResultSet;
import org.apache.flink.table.gateway.common.session.SessionEnvironment;
import org.apache.flink.table.gateway.common.session.SessionHandle;
import org.apache.flink.table.gateway.service.context.DefaultContext;
import org.apache.flink.table.gateway.service.session.SessionManager;
import org.apache.flink.table.gateway.service.utils.MockedEndpointVersion;
import org.apache.flink.table.planner.functions.casting.RowDataToStringConverterImpl;
import org.apache.flink.table.types.utils.DataTypeUtils;
import org.apache.flink.table.utils.print.PrintStyle;
import org.apache.flink.table.utils.print.TableauStyle;
import org.apache.flink.test.util.AbstractTestBase;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.ByteArrayOutputStream;
import java.io.PrintWriter;
import java.util.Collections;
import java.util.Iterator;

/** ITCase for {@link SQLGatewayServiceImpl}. */
public class SQLGatewayServiceITCase extends AbstractTestBase {

    private static SessionManager sessionManager;
    private static SqlGatewayService service;

    private static final int NUM_TMS = 2;
    private static final int NUM_SLOTS_PER_TM = 2;

    @ClassRule public static TemporaryFolder tempFolder = new TemporaryFolder();

    @BeforeClass
    public static void setup() {
        sessionManager =
                new SessionManager(
                        new DefaultContext(
                                new Configuration(MINI_CLUSTER_RESOURCE.getClientConfiguration())
                                        // Make sure we use the new cast behaviour
                                        .set(
                                                ExecutionConfigOptions
                                                        .TABLE_EXEC_LEGACY_CAST_BEHAVIOUR,
                                                ExecutionConfigOptions.LegacyCastBehaviour
                                                        .DISABLED),
                                Collections.singletonList(new DefaultCLI())));
        sessionManager.start();
        service = new SQLGatewayServiceImpl(sessionManager);
    }

    @AfterClass
    public static void cleanup() {
        if (sessionManager != null) {
            sessionManager.stop();
        }
    }

    @Test
    public void testCreate() throws Exception {
        SessionHandle sessionHandle =
                service.openSession(
                        SessionEnvironment.newBuilder()
                                .setSessionEndpointVersion(MockedEndpointVersion.V1)
                                .build());
        OperationHandle operationHandle =
                service.executeStatement(
                        sessionHandle,
                        "CREATE TABLE MyTable (\n"
                                + "  id INT,\n"
                                + "  name STRING) WITH (\n"
                                + "  'connector' = 'values'\n"
                                + ")",
                        -1,
                        new Configuration());

        waitOperationTerminate(sessionHandle, operationHandle);

        ResultSet resultSet = service.fetchResults(sessionHandle, operationHandle, 0, 500);
        System.out.println(resultSet.toString());

        operationHandle =
                service.executeStatement(sessionHandle, "SHOW TABLES", -1, new Configuration());

        waitOperationTerminate(sessionHandle, operationHandle);

        resultSet = service.fetchResults(sessionHandle, operationHandle, 0, 500);

        System.out.println(resultSet);

        service.closeSession(sessionHandle);
    }

    @Test
    public void testSelect() throws Exception {
        SessionHandle sessionHandle =
                service.openSession(
                        SessionEnvironment.newBuilder()
                                .setSessionEndpointVersion(MockedEndpointVersion.V1)
                                .build());
        OperationHandle operationHandle =
                service.executeStatement(
                        sessionHandle,
                        "SELECT id, COUNT(*) as cnt, COUNT(DISTINCT str) as uv, max(ts) as max_ts\n"
                                + "FROM (VALUES\n"
                                + "  (1, 'Hello World', TIMESTAMP '2021-04-13 20:12:11.123456789'),\n"
                                + "  (2, 'Hi', TIMESTAMP '2021-04-13 19:12:11.123456789'),\n"
                                + "  (2, 'Hi', TIMESTAMP '2021-04-13 21:12:11.123456789')) as T(id, str, ts)\n"
                                + "GROUP BY id;",
                        -1,
                        new Configuration());

        waitOperationTerminate(sessionHandle, operationHandle);
        ResultSet resultSet = service.fetchResults(sessionHandle, operationHandle, 0, 500);
        TableauStyle tableauStyle =
                PrintStyle.tableauWithTypeInferredColumnWidths(
                        // sinkOperation.getConsumedDataType() handles legacy types
                        DataTypeUtils.expandCompositeTypeToSchema(
                                resultSet.getResultSchema().toPhysicalRowDataType()),
                        new RowDataToStringConverterImpl(
                                resultSet.getResultSchema().toPhysicalRowDataType()),
                        PrintStyle.DEFAULT_MAX_COLUMN_WIDTH,
                        false,
                        true);

        ByteArrayOutputStream output = new ByteArrayOutputStream();
        tableauStyle.print(
                new RowDataIterator(sessionHandle, operationHandle), new PrintWriter(output));

        System.out.println(output);
        service.closeSession(sessionHandle);
    }

    private void waitOperationTerminate(
            SessionHandle sessionHandle, OperationHandle operationHandle) throws Exception {
        while (!service.getOperationInfo(sessionHandle, operationHandle)
                .getStatus()
                .isTerminalStatus()) {
            Thread.sleep(100);
        }
    }

    static class RowDataIterator implements Iterator<RowData> {

        private final SessionHandle sessionHandle;
        private final OperationHandle operationHandle;
        private Long nextToken = 0L;

        private Iterator<RowData> currentRowData;

        public RowDataIterator(SessionHandle sessionHandle, OperationHandle operationHandle) {
            this.sessionHandle = sessionHandle;
            this.operationHandle = operationHandle;
            ResultSet resultSet = service.fetchResults(sessionHandle, operationHandle, 0, 500);
            nextToken = resultSet.getNextToken();
            currentRowData = resultSet.getData().iterator();
        }

        @Override
        public boolean hasNext() {
            while (!currentRowData.hasNext()) {
                if (nextToken == null) {
                    return false;
                }
                ResultSet resultSet =
                        service.fetchResults(sessionHandle, operationHandle, nextToken, 500);
                nextToken = resultSet.getNextToken();
                currentRowData = resultSet.getData().iterator();
            }

            return true;
        }

        @Override
        public RowData next() {
            return currentRowData.next();
        }
    }
}
