/*
 * Copyright (c) 2025, WSO2 LLC. (http://www.wso2.org).
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
import software.amazon.awssdk.auth.credentials.InstanceProfileCredentialsProvider;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.profiles.ProfileFile;
import software.amazon.awssdk.services.redshiftdata.RedshiftDataClient;
import software.amazon.awssdk.services.redshiftdata.model.BatchExecuteStatementRequest;
import software.amazon.awssdk.services.redshiftdata.model.BatchExecuteStatementResponse;
import software.amazon.awssdk.services.redshiftdata.model.DescribeStatementRequest;
import software.amazon.awssdk.services.redshiftdata.model.DescribeStatementResponse;
import software.amazon.awssdk.services.redshiftdata.model.ExecuteStatementRequest;
import software.amazon.awssdk.services.redshiftdata.model.ExecuteStatementResponse;
import software.amazon.awssdk.services.redshiftdata.model.GetStatementResultRequest;
import software.amazon.awssdk.services.redshiftdata.model.GetStatementResultResponse;

import java.nio.file.Path;
import java.util.Objects;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Representation of {@link RedshiftDataClient} with
 * utility methods to invoke as inter-op functions.
 */
public class NativeClientAdaptor {
    static final String NATIVE_CLIENT = "nativeClient";
    private static final String NATIVE_DB_ACCESS_CONFIG = "nativeDbAccessConfig";
    private static final ExecutorService EXECUTOR_SERVICE = Executors
            .newCachedThreadPool(new RedshiftDataThreadFactory());

    private NativeClientAdaptor() {
    }

    public static Object init(BObject bClient, BMap<BString, Object> bConnectionConfig) {
        try {
            ConnectionConfig connectionConfig = new ConnectionConfig(bConnectionConfig);
            AwsCredentialsProvider credentialsProvider = getCredentialsProvider(connectionConfig.authConfig());
            RedshiftDataClient nativeClient = RedshiftDataClient.builder()
                    .region(connectionConfig.region())
                    .credentialsProvider(credentialsProvider)
                    .build();
            bClient.addNativeData(NATIVE_CLIENT, nativeClient);
            bClient.addNativeData(NATIVE_DB_ACCESS_CONFIG, connectionConfig.dbAccessConfig());
        } catch (Exception e) {
            String errorMsg = String.format("Error occurred while initializing the Redshift client: %s",
                    e.getMessage());
            return CommonUtils.createError(errorMsg, e);
        }
        return null;
    }

    private static AwsCredentialsProvider getCredentialsProvider(Object authConfig) {
        if (authConfig instanceof StaticAuthConfig staticAuth) {
            AwsCredentials credentials = Objects.nonNull(staticAuth.sessionToken()) ?
                    AwsSessionCredentials.create(
                            staticAuth.accessKeyId(), staticAuth.secretAccessKey(), staticAuth.sessionToken()) :
                    AwsBasicCredentials.create(staticAuth.accessKeyId(), staticAuth.secretAccessKey());
            return StaticCredentialsProvider.create(credentials);
        }
        InstanceProfileCredentials instanceProfileCredentials = (InstanceProfileCredentials) authConfig;
        InstanceProfileCredentialsProvider.Builder instanceCredentialBuilder =
                InstanceProfileCredentialsProvider.builder();
        if (Objects.nonNull(instanceProfileCredentials.profileName())) {
            instanceCredentialBuilder.profileName(instanceProfileCredentials.profileName());
        }
        if (Objects.nonNull(instanceProfileCredentials.profileFile())) {
            instanceCredentialBuilder.profileFile(ProfileFile.builder()
                    .content(Path.of(instanceProfileCredentials.profileFile()))
                    .type(ProfileFile.Type.CONFIGURATION)
                    .build());
        }
        return instanceCredentialBuilder.build();
    }

    @SuppressWarnings("unchecked")
    public static Object execute(Environment env, BObject bClient, BObject bSqlStatement,
                                 BMap<BString, Object> bExecutionConfig) {
        RedshiftDataClient nativeClient = (RedshiftDataClient) bClient.getNativeData(NATIVE_CLIENT);
        Object initLevelDbAccessConfig = bClient.getNativeData(NATIVE_DB_ACCESS_CONFIG);
        Future future = env.markAsync();
        EXECUTOR_SERVICE.execute(() -> {
            try {
                ExecuteStatementRequest executeRequest = CommonUtils.getNativeExecuteRequest(
                        bSqlStatement, bExecutionConfig, initLevelDbAccessConfig);
                ExecuteStatementResponse executionResponse = nativeClient
                        .executeStatement(executeRequest);
                BMap<BString, Object> bResponse = CommonUtils.getExecutionResponse(executionResponse);
                future.complete(bResponse);
            } catch (Exception e) {
                String errorMsg = String.format("Error occurred while executing the execute: %s",
                        Objects.requireNonNullElse(e.getMessage(), "Unknown error"));
                BError bError = CommonUtils.createError(errorMsg, e);
                future.complete(bError);
            }
        });
        return null;
    }

    @SuppressWarnings("unchecked")
    public static Object batchExecute(Environment env, BObject bClient, BArray bSqlStatements,
                                      BMap<BString, Object> bExecutionConfig) {
        RedshiftDataClient nativeClient = (RedshiftDataClient) bClient.getNativeData(NATIVE_CLIENT);
        Object initLevelDbAccessConfig = bClient.getNativeData(NATIVE_DB_ACCESS_CONFIG);
        Future future = env.markAsync();
        EXECUTOR_SERVICE.execute(() -> {
            try {
                BatchExecuteStatementRequest batchExecuteStatementRequest = CommonUtils
                        .getNativeBatchExecuteRequest(
                                bSqlStatements, bExecutionConfig, initLevelDbAccessConfig);
                BatchExecuteStatementResponse batchExecutionResponse = nativeClient
                        .batchExecuteStatement(batchExecuteStatementRequest);
                BMap<BString, Object> bResponse = CommonUtils
                        .getBatchExecutionResponse(batchExecutionResponse);
                future.complete(bResponse);
            } catch (Exception e) {
                String errorMsg = String.format("Error occurred while executing the batchExecute: %s",
                        Objects.requireNonNullElse(e.getMessage(), "Unknown error"));
                BError bError = CommonUtils.createError(errorMsg, e);
                future.complete(bError);
            }
        });
        return null;
    }

    @SuppressWarnings("unchecked")
    public static Object describe(Environment env, BObject bClient, BString bStatementId) {
        RedshiftDataClient nativeClient = (RedshiftDataClient) bClient.getNativeData(NATIVE_CLIENT);
        String statementId = bStatementId.getValue();
        Future future = env.markAsync();
        EXECUTOR_SERVICE.execute(() -> {
            try {
                DescribeStatementResponse describeStatementResponse = nativeClient.describeStatement(
                        DescribeStatementRequest.builder().id(statementId).build());
                BMap<BString, Object> bResponse = CommonUtils.getDescriptionResponse(describeStatementResponse);
                future.complete(bResponse);
            } catch (Exception e) {
                String errorMsg = String.format("Error occurred while executing the describe: %s",
                        Objects.requireNonNullElse(e.getMessage(), "Unknown error"));
                BError bError = CommonUtils.createError(errorMsg, e);
                future.complete(bError);
            }
        });
        return null;
    }

    public static Object getResultAsStream(Environment env, BObject bClient, BString bStatementId,
                                           BTypedesc recordType) {
        RedshiftDataClient nativeClient = (RedshiftDataClient) bClient.getNativeData(NATIVE_CLIENT);
        String statementId = bStatementId.getValue();
        Future future = env.markAsync();
        EXECUTOR_SERVICE.execute(() -> {
            try {
                GetStatementResultResponse nativeResultResponse = nativeClient
                        .getStatementResult(GetStatementResultRequest.builder().id(statementId).build());
                BStream resultStream = QueryResultProcessor
                        .getRecordStream(nativeClient, statementId, nativeResultResponse, recordType);
                future.complete(resultStream);
            } catch (Exception e) {
                String errorMsg = String.format("Error occurred while executing the getResultAsStream: %s",
                        Objects.requireNonNullElse(e.getMessage(), "Unknown error"));
                BError bError = CommonUtils.createError(errorMsg, e);
                future.complete(bError);
            }
        });
        return null;
    }

    public static Object close(BObject bClient) {
        RedshiftDataClient nativeClient = (RedshiftDataClient) bClient.getNativeData(NATIVE_CLIENT);
        try {
            nativeClient.close();
        } catch (Exception e) {
            String errorMsg = String.format("Error occurred while closing the Redshift client: %s",
                    Objects.requireNonNullElse(e.getMessage(), "Unknown error"));
            return CommonUtils.createError(errorMsg, e);
        }
        return null;
    }
}
