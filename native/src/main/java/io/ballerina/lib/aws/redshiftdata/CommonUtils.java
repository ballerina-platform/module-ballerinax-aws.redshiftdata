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

import io.ballerina.runtime.api.creators.ErrorCreator;
import io.ballerina.runtime.api.creators.TypeCreator;
import io.ballerina.runtime.api.creators.ValueCreator;
import io.ballerina.runtime.api.types.ArrayType;
import io.ballerina.runtime.api.utils.StringUtils;
import io.ballerina.runtime.api.values.BArray;
import io.ballerina.runtime.api.values.BError;
import io.ballerina.runtime.api.values.BMap;
import io.ballerina.runtime.api.values.BObject;
import io.ballerina.runtime.api.values.BString;
import io.ballerina.stdlib.time.nativeimpl.Utc;
import software.amazon.awssdk.awscore.exception.AwsErrorDetails;
import software.amazon.awssdk.awscore.exception.AwsServiceException;
import software.amazon.awssdk.http.SdkHttpResponse;
import software.amazon.awssdk.services.redshiftdata.model.BatchExecuteStatementRequest;
import software.amazon.awssdk.services.redshiftdata.model.BatchExecuteStatementResponse;
import software.amazon.awssdk.services.redshiftdata.model.DescribeStatementResponse;
import software.amazon.awssdk.services.redshiftdata.model.ExecuteStatementRequest;
import software.amazon.awssdk.services.redshiftdata.model.ExecuteStatementResponse;
import software.amazon.awssdk.services.redshiftdata.model.SubStatementData;

import java.math.BigDecimal;
import java.util.Objects;

import static io.ballerina.lib.aws.redshiftdata.Cluster.CLUSTER_ID;
import static io.ballerina.lib.aws.redshiftdata.ConnectionConfig.CONNECTION_CONFIG_DB_ACCESS_CONFIG;
import static io.ballerina.lib.aws.redshiftdata.WorkGroup.WORK_GROUP_NAME;

/**
 * {@code CommonUtils} contains the common utility functions for the Ballerina
 * AWS Redshift Data API Client.
 */
public final class CommonUtils {
    // Constants related to `Error`
    private static final String ERROR = "Error";
    private static final String ERROR_DETAILS = "ErrorDetails";
    private static final BString ERROR_DETAILS_HTTP_STATUS_CODE = StringUtils.fromString("httpStatusCode");
    private static final BString ERROR_DETAILS_HTTP_STATUS_TEXT = StringUtils.fromString("httpStatusText");
    private static final BString ERROR_DETAILS_ERROR_CODE = StringUtils.fromString("errorCode");
    private static final BString ERROR_DETAILS_ERROR_MESSAGE = StringUtils.fromString("errorMessage");

    // Constants related to `DescriptionResponse`
    private static final String DESCRIPTION_RES_RECORD = "DescriptionResponse";
    private static final BString DESCRIPTION_RES_SUB_STATEMENTS = StringUtils.fromString("subStatements");
    private static final BString DESCRIPTION_RES_REDSHIFT_PID = StringUtils.fromString("redshiftPid");
    private static final BString DESCRIPTION_RES_SESSION_ID = StringUtils.fromString("sessionId");

    // Constants related to `StatementData`
    private static final String STATEMENT_DATA_RECORD = "StatementData";
    private static final BString STATEMENT_DATA_STATEMENT_ID = StringUtils.fromString("statementId");
    private static final BString STATEMENT_DATA_CREATED_AT = StringUtils.fromString("createdAt");
    private static final BString STATEMENT_DATA_DURATION = StringUtils.fromString("duration");
    private static final BString STATEMENT_DATA_ERROR = StringUtils.fromString("error");
    private static final BString STATEMENT_DATA_HAS_RESULT_SET = StringUtils.fromString("hasResultSet");
    private static final BString STATEMENT_DATA_QUERY_STRING = StringUtils.fromString("queryString");
    private static final BString STATEMENT_DATA_REDSHIFT_QUERY_ID = StringUtils.fromString("redshiftQueryId");
    private static final BString STATEMENT_DATA_RESULT_ROWS = StringUtils.fromString("resultRows");
    private static final BString STATEMENT_DATA_RESULT_SIZE = StringUtils.fromString("resultSize");
    private static final BString STATEMENT_DATA_STATUS = StringUtils.fromString("status");
    private static final BString STATEMENT_DATA_UPDATED_AT = StringUtils.fromString("updatedAt");

    // Constants related to `ExecutionConfig`
    private static final BString EXECUTE_STATEMENT_CONFIG_CLIENT_TOKEN = StringUtils.fromString("clientToken");
    private static final BString EXECUTE_STATEMENT_CONFIG_STATEMENT_NAME = StringUtils.fromString("statementName");
    private static final BString EXECUTE_STATEMENT_CONFIG_WITH_EVENT = StringUtils.fromString("withEvent");

    // Constants related to `ExecutionResponse`
    private static final String EXECUTE_STATEMENT_RES_RECORD = "ExecutionResponse";
    private static final BString EXECUTE_STATEMENT_RES_CREATE_AT = StringUtils.fromString("createdAt");
    private static final BString EXECUTE_STATEMENT_RES_DB_GROUPS = StringUtils.fromString("dbGroups");
    private static final BString EXECUTE_STATEMENT_RES_STATEMENT_ID = StringUtils.fromString("statementId");
    private static final BString EXECUTE_STATEMENT_RES_SESSION_ID = StringUtils.fromString("sessionId");

    private CommonUtils() {
    }

    public static BError createError(String message, Throwable exception) {
        BError cause = ErrorCreator.createError(exception);
        BMap<BString, Object> errorDetails = ValueCreator.createRecordValue(
                ModuleUtils.getModule(), ERROR_DETAILS);
        if (exception instanceof AwsServiceException awsServiceException &&
                Objects.nonNull(awsServiceException.awsErrorDetails())) {
            AwsErrorDetails awsErrorDetails = awsServiceException.awsErrorDetails();
            SdkHttpResponse sdkResponse = awsErrorDetails.sdkHttpResponse();
            if (Objects.nonNull(sdkResponse)) {
                errorDetails.put(ERROR_DETAILS_HTTP_STATUS_CODE, sdkResponse.statusCode());
                sdkResponse.statusText().ifPresent(httpStatusTxt -> errorDetails.put(
                        ERROR_DETAILS_HTTP_STATUS_TEXT, StringUtils.fromString(httpStatusTxt)));
            }
            errorDetails.put(ERROR_DETAILS_ERROR_CODE, StringUtils.fromString(awsErrorDetails.errorCode()));
            errorDetails.put(ERROR_DETAILS_ERROR_MESSAGE, StringUtils.fromString(awsErrorDetails.errorMessage()));
        }
        return ErrorCreator.createError(
                ModuleUtils.getModule(), ERROR, StringUtils.fromString(message), cause, errorDetails);
    }

    @SuppressWarnings("unchecked")
    public static ExecuteStatementRequest getNativeExecuteRequest(
            BObject bSqlStatement, BMap<BString, Object> bConfig, Object initLevelDbAccessConfig) throws Exception {
        ExecuteStatementRequest.Builder builder = ExecuteStatementRequest.builder();

        // Set the SQL statement
        ParameterizedQuery parameterizedQuery = new ParameterizedQuery(bSqlStatement);
        builder.sql(parameterizedQuery.getQueryString());
        if (parameterizedQuery.hasParameters()) {
            builder.parameters(parameterizedQuery.getParameters());
        }

        // If a `dbAccessConfig` is provided in the ExecutionConfig , it will override the init level dbAccessConfig.
        Object dbAccessConfig = validateAndGetDbAccessConfig(bConfig, initLevelDbAccessConfig);

        // Set the database access configurations
        if (dbAccessConfig instanceof Cluster cluster) {
            builder.database(cluster.database());
            builder.clusterIdentifier(cluster.id());
            if (cluster.dbUser() != null) {
                builder.dbUser(cluster.dbUser());
            } else {
                builder.secretArn(cluster.secretArn());
            }
            if (cluster.sessionKeepAliveSeconds() != null) {
                builder.sessionKeepAliveSeconds(cluster.sessionKeepAliveSeconds());
            }
        } else if (dbAccessConfig instanceof WorkGroup workGroup) {
            builder.workgroupName(workGroup.name());
            builder.database(workGroup.database());
            builder.secretArn(workGroup.secretArn());
            if (workGroup.sessionKeepAliveSeconds() != null) {
                builder.sessionKeepAliveSeconds(workGroup.sessionKeepAliveSeconds());
            }
        } else {
            builder.sessionId((String) dbAccessConfig);
        }

        // Set other configurations
        if (bConfig.containsKey(EXECUTE_STATEMENT_CONFIG_CLIENT_TOKEN)) {
            builder.clientToken(bConfig.getStringValue(EXECUTE_STATEMENT_CONFIG_CLIENT_TOKEN).getValue());
        }
        if (bConfig.containsKey(EXECUTE_STATEMENT_CONFIG_STATEMENT_NAME)) {
            builder.statementName(bConfig.getStringValue(EXECUTE_STATEMENT_CONFIG_STATEMENT_NAME).getValue());
        }
        if (bConfig.containsKey(EXECUTE_STATEMENT_CONFIG_WITH_EVENT)) {
            builder.withEvent(bConfig.getBooleanValue(EXECUTE_STATEMENT_CONFIG_WITH_EVENT));
        }
        return builder.build();
    }

    public static BMap<BString, Object> getExecutionResponse(ExecuteStatementResponse nativeResponse) {
        BMap<BString, Object> response = ValueCreator.createRecordValue(
                ModuleUtils.getModule(), EXECUTE_STATEMENT_RES_RECORD);

        if (nativeResponse.hasDbGroups()) {
            BString[] dbGroups = nativeResponse.dbGroups().stream()
                    .map(StringUtils::fromString)
                    .toArray(BString[]::new);
            response.put(EXECUTE_STATEMENT_RES_DB_GROUPS, ValueCreator.createArrayValue(dbGroups));
        }
        response.put(EXECUTE_STATEMENT_RES_CREATE_AT, new Utc(nativeResponse.createdAt()).build());
        response.put(EXECUTE_STATEMENT_RES_STATEMENT_ID, StringUtils.fromString(nativeResponse.id()));
        if (Objects.nonNull(nativeResponse.sessionId())) {
            response.put(EXECUTE_STATEMENT_RES_SESSION_ID, StringUtils.fromString(nativeResponse.sessionId()));
        }
        return response;
    }

    @SuppressWarnings("unchecked")
    public static BatchExecuteStatementRequest getNativeBatchExecuteRequest(
            BArray bSqlStatements, BMap<BString, Object> bConfig, Object initLevelDbAccessConfig) throws Exception {
        BatchExecuteStatementRequest.Builder builder = BatchExecuteStatementRequest.builder();

        // Set the SQL statements
        String[] sqlStatements = new String[bSqlStatements.size()];
        for (int i = 0; i < bSqlStatements.size(); i++) {
            sqlStatements[i] = new ParameterizedQuery((BObject) bSqlStatements.get(i)).getPreparedQuery();
        }
        builder.sqls(sqlStatements);

        // If a `dbAccessConfig` is provided in the ExecutionConfig , it will override the init level dbAccessConfig.
        Object dbAccessConfig = validateAndGetDbAccessConfig(bConfig, initLevelDbAccessConfig);

        // Set the database access configurations
        if (dbAccessConfig instanceof Cluster cluster) {
            builder.database(cluster.database());
            builder.clusterIdentifier(cluster.id());
            if (cluster.dbUser() != null) {
                builder.dbUser(cluster.dbUser());
            } else {
                builder.secretArn(cluster.secretArn());
            }
            if (cluster.sessionKeepAliveSeconds() != null) {
                builder.sessionKeepAliveSeconds(cluster.sessionKeepAliveSeconds());
            }
        } else if (dbAccessConfig instanceof WorkGroup workGroup) {
            builder.workgroupName(workGroup.name());
            builder.database(workGroup.database());
            builder.secretArn(workGroup.secretArn());
            if (workGroup.sessionKeepAliveSeconds() != null) {
                builder.sessionKeepAliveSeconds(workGroup.sessionKeepAliveSeconds());
            }
        } else {
            builder.sessionId((String) dbAccessConfig);
        }

        // Set other configurations
        if (bConfig.containsKey(EXECUTE_STATEMENT_CONFIG_CLIENT_TOKEN)) {
            builder.clientToken(bConfig.getStringValue(EXECUTE_STATEMENT_CONFIG_CLIENT_TOKEN).getValue());
        }
        if (bConfig.containsKey(EXECUTE_STATEMENT_CONFIG_STATEMENT_NAME)) {
            builder.statementName(bConfig.getStringValue(EXECUTE_STATEMENT_CONFIG_STATEMENT_NAME).getValue());
        }
        if (bConfig.containsKey(EXECUTE_STATEMENT_CONFIG_WITH_EVENT)) {
            builder.withEvent(bConfig.getBooleanValue(EXECUTE_STATEMENT_CONFIG_WITH_EVENT));
        }
        return builder.build();
    }

    @SuppressWarnings("unchecked")
    private static Object validateAndGetDbAccessConfig(BMap<BString, Object> bConfig, Object initLevelDbAccessConfig)
            throws Exception {
        Object dbAccessConfig = initLevelDbAccessConfig;
        if (bConfig.containsKey(CONNECTION_CONFIG_DB_ACCESS_CONFIG)) {
            Object bDbAccessConfigObj = bConfig.get(CONNECTION_CONFIG_DB_ACCESS_CONFIG);

            if (bDbAccessConfigObj instanceof BString bSessionId) {
                dbAccessConfig = bSessionId.getValue();
            } else {
                BMap<BString, Object> bDbAccessConfig = (BMap<BString, Object>) bDbAccessConfigObj;
                if (bDbAccessConfig.containsKey(CLUSTER_ID)) {
                    dbAccessConfig = new Cluster(bDbAccessConfig);
                } else if (bDbAccessConfig.containsKey(WORK_GROUP_NAME)) {
                    dbAccessConfig = new WorkGroup(bDbAccessConfig);
                }
            }
        }
        if (Objects.isNull(dbAccessConfig)) {
            throw new Exception("No database access configuration provided in the initialization " +
                    "of the client or in the execute statement config");
        }
        return dbAccessConfig;
    }

    public static BMap<BString, Object> getBatchExecutionResponse(BatchExecuteStatementResponse nativeResponse) {
        BMap<BString, Object> response = ValueCreator.createRecordValue(
                ModuleUtils.getModule(), EXECUTE_STATEMENT_RES_RECORD);

        if (nativeResponse.hasDbGroups()) {
            BString[] dbGroups = nativeResponse.dbGroups().stream()
                    .map(StringUtils::fromString)
                    .toArray(BString[]::new);
            response.put(EXECUTE_STATEMENT_RES_DB_GROUPS, ValueCreator.createArrayValue(dbGroups));
        }
        response.put(EXECUTE_STATEMENT_RES_CREATE_AT, new Utc(nativeResponse.createdAt()).build());
        response.put(EXECUTE_STATEMENT_RES_STATEMENT_ID, StringUtils.fromString(nativeResponse.id()));
        if (Objects.nonNull(nativeResponse.sessionId())) {
            response.put(EXECUTE_STATEMENT_RES_SESSION_ID, StringUtils.fromString(nativeResponse.sessionId()));
        }
        return response;
    }

    public static BMap<BString, Object> getDescriptionResponse(DescribeStatementResponse nativeResponse) {
        BMap<BString, Object> response = ValueCreator.createRecordValue(
                ModuleUtils.getModule(), DESCRIPTION_RES_RECORD);

        if (nativeResponse.hasSubStatements()) {
            ArrayType subStatementDataArrayType = TypeCreator.createArrayType(ValueCreator.createRecordValue(
                    ModuleUtils.getModule(), STATEMENT_DATA_RECORD).getType());
            BArray subStatementsArray = ValueCreator.createArrayValue(subStatementDataArrayType);
            for (SubStatementData subStatementData : nativeResponse.subStatements()) {
                subStatementsArray.append(getSubStatementData(subStatementData));
            }
            response.put(DESCRIPTION_RES_SUB_STATEMENTS, subStatementsArray);
        }
        response.put(DESCRIPTION_RES_REDSHIFT_PID, nativeResponse.redshiftPid());
        if (Objects.nonNull(nativeResponse.sessionId())) {
            response.put(DESCRIPTION_RES_SESSION_ID,
                    StringUtils.fromString(nativeResponse.sessionId()));
        }

        // Set the statement data
        response.put(STATEMENT_DATA_STATEMENT_ID, StringUtils.fromString(nativeResponse.id()));
        response.put(STATEMENT_DATA_CREATED_AT, new Utc(nativeResponse.createdAt()).build());
        response.put(STATEMENT_DATA_UPDATED_AT, new Utc(nativeResponse.updatedAt()).build());
        response.put(STATEMENT_DATA_STATUS, StringUtils.fromString(nativeResponse.statusAsString()));
        response.put(STATEMENT_DATA_HAS_RESULT_SET, nativeResponse.hasResultSet());
        response.put(STATEMENT_DATA_REDSHIFT_QUERY_ID, nativeResponse.redshiftQueryId());
        response.put(STATEMENT_DATA_RESULT_ROWS, nativeResponse.resultRows());
        response.put(STATEMENT_DATA_RESULT_SIZE, nativeResponse.resultSize());
        // Convert the duration from nanoseconds to seconds
        response.put(STATEMENT_DATA_DURATION,
                ValueCreator.createDecimalValue(convertNanosToSeconds(nativeResponse.duration())));
        if (Objects.nonNull(nativeResponse.queryString())) {
            response.put(STATEMENT_DATA_QUERY_STRING, StringUtils.fromString(nativeResponse.queryString()));
        }
        if (Objects.nonNull(nativeResponse.error())) {
            response.put(STATEMENT_DATA_ERROR, StringUtils.fromString(nativeResponse.error()));
        }
        return response;
    }

    private static BMap<BString, Object> getSubStatementData(SubStatementData subStatementData) {
        BMap<BString, Object> record = ValueCreator.createRecordValue(
                ModuleUtils.getModule(), STATEMENT_DATA_RECORD);
        record.put(STATEMENT_DATA_STATEMENT_ID, StringUtils.fromString(subStatementData.id()));
        record.put(STATEMENT_DATA_CREATED_AT, new Utc(subStatementData.createdAt()).build());
        record.put(STATEMENT_DATA_UPDATED_AT, new Utc(subStatementData.updatedAt()).build());
        record.put(STATEMENT_DATA_STATUS, StringUtils.fromString(subStatementData.statusAsString()));
        record.put(STATEMENT_DATA_HAS_RESULT_SET, subStatementData.hasResultSet());
        record.put(STATEMENT_DATA_REDSHIFT_QUERY_ID, subStatementData.redshiftQueryId());
        record.put(STATEMENT_DATA_RESULT_ROWS, subStatementData.resultRows());
        record.put(STATEMENT_DATA_RESULT_SIZE, subStatementData.resultSize());
        // Convert the duration from nanoseconds to seconds
        record.put(STATEMENT_DATA_DURATION,
                ValueCreator.createDecimalValue(convertNanosToSeconds(subStatementData.duration())));
        if (Objects.nonNull(subStatementData.queryString())) {
            record.put(STATEMENT_DATA_QUERY_STRING, StringUtils.fromString(subStatementData.queryString()));
        }
        if (Objects.nonNull(subStatementData.error())) {
            record.put(STATEMENT_DATA_ERROR, StringUtils.fromString(subStatementData.error()));
        }
        return record;
    }

    private static BigDecimal convertNanosToSeconds(long nanos) {
        return BigDecimal.valueOf(nanos).divide(BigDecimal.valueOf(1_000_000_000));
    }
}
