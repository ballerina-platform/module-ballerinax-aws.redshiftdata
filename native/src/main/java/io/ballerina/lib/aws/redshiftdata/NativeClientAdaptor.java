/*
 * Copyright (c) 2024, WSO2 LLC. (http://www.wso2.org).
 *
 * WSO2 LLC. licenses this file to you under the Apache License,
 * Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.ballerina.lib.aws.redshiftdata;

import io.ballerina.runtime.api.Environment;
import io.ballerina.runtime.api.Future;
import io.ballerina.runtime.api.values.BArray;
import io.ballerina.runtime.api.values.BError;
import io.ballerina.runtime.api.values.BMap;
import io.ballerina.runtime.api.values.BObject;
import io.ballerina.runtime.api.values.BStream;
import io.ballerina.runtime.api.values.BString;
import io.ballerina.runtime.api.values.BTypedesc;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.AwsSessionCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.services.redshiftdata.RedshiftDataClient;
import software.amazon.awssdk.services.redshiftdata.model.BatchExecuteStatementRequest;
import software.amazon.awssdk.services.redshiftdata.model.BatchExecuteStatementResponse;
import software.amazon.awssdk.services.redshiftdata.model.DescribeStatementRequest;
import software.amazon.awssdk.services.redshiftdata.model.DescribeStatementResponse;
import software.amazon.awssdk.services.redshiftdata.model.ExecuteStatementRequest;
import software.amazon.awssdk.services.redshiftdata.model.ExecuteStatementResponse;
import software.amazon.awssdk.services.redshiftdata.model.GetStatementResultRequest;
import software.amazon.awssdk.services.redshiftdata.model.SubStatementData;
import software.amazon.awssdk.services.redshiftdata.paginators.GetStatementResultIterable;

import java.math.BigDecimal;
import java.util.Objects;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Representation of {@link RedshiftDataClient} with
 * utility methods to invoke as inter-op functions.
 */
public class NativeClientAdaptor {
    private static final ExecutorService EXECUTOR_SERVICE = Executors
            .newCachedThreadPool(new RedshiftDataThreadFactory());

    private NativeClientAdaptor() {
    }

    public static Object init(BObject bClient, BMap<BString, Object> bConnectionConfig) {
        try {
            ConnectionConfig connectionConfig = new ConnectionConfig(bConnectionConfig);
            AwsCredentials credentials = getCredentials(connectionConfig.authConfig());
            AwsCredentialsProvider credentialsProvider = StaticCredentialsProvider.create(credentials);
            RedshiftDataClient nativeClient = RedshiftDataClient.builder()
                    .region(connectionConfig.region())
                    .credentialsProvider(credentialsProvider)
                    .build();
            bClient.addNativeData(Constants.NATIVE_CLIENT, nativeClient);
            bClient.addNativeData(Constants.NATIVE_DATABASE_CONFIG, connectionConfig.databaseConfig());
        } catch (Exception e) {
            String errorMsg = String.format("Error occurred while initializing the Redshift client: %s",
                    e.getMessage());
            return CommonUtils.createError(errorMsg, e);
        }
        return null;
    }

    private static AwsCredentials getCredentials(AuthConfig authConfig) {
        if (Objects.nonNull(authConfig.sessionToken())) {
            return AwsSessionCredentials.create(authConfig.accessKeyId(), authConfig.secretAccessKey(),
                    authConfig.sessionToken());
        } else {
            return AwsBasicCredentials.create(authConfig.accessKeyId(), authConfig.secretAccessKey());
        }
    }

    @SuppressWarnings("unchecked")
    public static Object executeStatement(Environment env, BObject bClient, BObject bSqlStatement,
                                          BMap<BString, Object> bExecuteStatementConfig) {
        RedshiftDataClient nativeClient = (RedshiftDataClient) bClient.getNativeData(Constants.NATIVE_CLIENT);
        DatabaseConfig databaseConfig = (DatabaseConfig) bClient.getNativeData(Constants.NATIVE_DATABASE_CONFIG);
        ExecuteStatementRequest executeStatementRequest = CommonUtils.getNativeExecuteStatementRequest(
                bSqlStatement, bExecuteStatementConfig, databaseConfig);
        Future future = env.markAsync();
        EXECUTOR_SERVICE.execute(() -> {
            try {
                ExecuteStatementResponse executeStatementResponse = nativeClient
                        .executeStatement(executeStatementRequest);
                BMap<BString, Object> bResponse = CommonUtils.getExecuteStatementResponse(executeStatementResponse);
                future.complete(bResponse);
            } catch (Exception e) {
                String errorMsg = String.format("Error occurred while executing execute-statement request: %s",
                        e.getMessage());
                BError bError = CommonUtils.createError(errorMsg, e);
                future.complete(bError);
            }
        });
        return null;
    }

    @SuppressWarnings("unchecked")
    public static Object batchExecuteStatement(Environment env, BObject bClient, BArray bSqlStatements,
                                               BMap<BString, Object> bExecuteStatementConfig) {
        RedshiftDataClient nativeClient = (RedshiftDataClient) bClient.getNativeData(Constants.NATIVE_CLIENT);
        DatabaseConfig databaseConfig = (DatabaseConfig) bClient.getNativeData(Constants.NATIVE_DATABASE_CONFIG);
        BatchExecuteStatementRequest batchExecuteStatementRequest = CommonUtils.getNativeBatchExecuteStatementRequest(
                bSqlStatements, bExecuteStatementConfig, databaseConfig);
        Future future = env.markAsync();
        EXECUTOR_SERVICE.execute(() -> {
            try {
                BatchExecuteStatementResponse batchExecuteStatementResponse = nativeClient
                        .batchExecuteStatement(batchExecuteStatementRequest);
                DescribeStatementResponse describeStatementResponse = nativeClient
                        .describeStatement(DescribeStatementRequest.builder()
                                .id(batchExecuteStatementResponse.id())
                                .build());
                String[] subStatementIds = describeStatementResponse.subStatements().stream()
                        .map(SubStatementData::id)
                        .toArray(String[]::new);
                BMap<BString, Object> bResponse = CommonUtils
                        .getBatchExecuteStatementResponse(batchExecuteStatementResponse, subStatementIds);
                future.complete(bResponse);
            } catch (Exception e) {
                String errorMsg = String.format("Error occurred while executing the batchExecuteStatement: %s",
                        e.getMessage());
                BError bError = CommonUtils.createError(errorMsg, e);
                future.complete(bError);
            }
        });
        return null;
    }

    @SuppressWarnings("unchecked")
    public static Object getExecutionResult(Environment env, BObject bClient, BString bStatementId,
                                               BMap<BString, Object> bResultRequest) {
        RedshiftDataClient nativeClient = (RedshiftDataClient) bClient.getNativeData(Constants.NATIVE_CLIENT);
        String statementId = bStatementId.getValue();
        RetrieveResultConfig retrieveResultConfig = new RetrieveResultConfig(bResultRequest);
        Future future = env.markAsync();
        EXECUTOR_SERVICE.execute(() -> {
            try {
                DescribeStatementResponse describeStatementResponse = getDescribeStatement(nativeClient,
                        statementId, retrieveResultConfig.timeout(), retrieveResultConfig.pollingInterval());
                BMap<BString, Object> bResponse = CommonUtils.getExecutionResultResponse(describeStatementResponse);
                future.complete(bResponse);
            } catch (Exception e) {
                String errorMsg = String.format("Error occurred while executing the getExecutionResult: %s",
                        e.getMessage());
                BError bError = CommonUtils.createError(errorMsg, e);
                future.complete(bError);
            }
        });
        return null;
    }

    public static Object getQueryResult(Environment env, BObject bClient, BString bStatementId,
                                        BTypedesc recordType, BMap<BString, Object> bResultConfig) {
        RedshiftDataClient nativeClient = (RedshiftDataClient) bClient.getNativeData(Constants.NATIVE_CLIENT);
        String statementId = bStatementId.getValue();
        RetrieveResultConfig retrieveResultConfig = new RetrieveResultConfig(bResultConfig);
        Future future = env.markAsync();
        EXECUTOR_SERVICE.execute(() -> {
            try {
                String mainStatementId = extractMainStatementId(statementId);
                // Wait for the statement to complete within the specified timeout
                DescribeStatementResponse describeStatementResponse = getDescribeStatement(nativeClient,
                        mainStatementId, retrieveResultConfig.timeout(), retrieveResultConfig.pollingInterval());
                if (!describeStatementResponse.hasResultSet()) {
                    throw new RuntimeException("Query result is not available for the statement: " + statementId);
                }

                GetStatementResultIterable nativeResultIterable = nativeClient
                        .getStatementResultPaginator(GetStatementResultRequest.builder().id(statementId).build());

                BStream resultStream = QueryResultProcessor.getRecordStream(nativeResultIterable, recordType);

                future.complete(resultStream);
            } catch (Exception e) {
                String errorMsg = String.format("Error occurred while getting the query result:\n %s",
                        e.getMessage());
                BError bError = CommonUtils.createError(errorMsg, e);
                future.complete(bError);
            }
        });
        return null;
    }

    public static Object close(BObject bClient) {
        RedshiftDataClient nativeClient = (RedshiftDataClient) bClient.getNativeData(Constants.NATIVE_CLIENT);
        try {
            nativeClient.close();
        } catch (Exception e) {
            String errorMsg = String.format("Error occurred while closing the Redshift client: %s",
                    e.getMessage());
            return CommonUtils.createError(errorMsg, e);
        }
        return null;
    }

    // helper methods
    private static DescribeStatementResponse getDescribeStatement(RedshiftDataClient nativeClient,
                                                                  String statementId, BigDecimal timeout,
                                                                  BigDecimal pollInterval) {
        // convert seconds to milliseconds
        long timeoutMillis = timeout.multiply(BigDecimal.valueOf(1000)).longValue();
        long pollIntervalMillis = pollInterval.multiply(BigDecimal.valueOf(1000)).longValue();
        long startTime = System.currentTimeMillis();
        DescribeStatementRequest describeStatementRequest = DescribeStatementRequest.builder()
                .id(statementId)
                .build();
        DescribeStatementResponse response;
        while ((System.currentTimeMillis() - startTime) < timeoutMillis) {
            response = nativeClient.describeStatement(describeStatementRequest);
            switch (response.status()) {
                case FINISHED:
                    return response;
                case FAILED:
                    throw new RuntimeException("Statement execution failed: " + response.error());
                case ABORTED:
                    throw new RuntimeException("Statement execution aborted");
                default:
                    try {
                        Thread.sleep(pollIntervalMillis);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
            }
        }
        throw new RuntimeException("Statement execution timed out");
    }

    private static String extractMainStatementId(String statementId) {
        int colonIndex = statementId.indexOf(':');
        return (colonIndex != -1) ? statementId.substring(0, colonIndex) : statementId;
    }
}
