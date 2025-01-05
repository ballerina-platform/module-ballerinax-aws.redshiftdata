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
import software.amazon.awssdk.services.redshiftdata.model.BatchExecuteStatementRequest;
import software.amazon.awssdk.services.redshiftdata.model.BatchExecuteStatementResponse;
import software.amazon.awssdk.services.redshiftdata.model.DescribeStatementResponse;
import software.amazon.awssdk.services.redshiftdata.model.ExecuteStatementRequest;
import software.amazon.awssdk.services.redshiftdata.model.ExecuteStatementResponse;
import software.amazon.awssdk.services.redshiftdata.model.SubStatementData;

import java.math.BigDecimal;
import java.util.Arrays;
import java.util.Objects;

/**
 * {@code CommonUtils} contains the common utility functions for the Ballerina
 * AWS Redshift Data Client.
 */
public final class CommonUtils {

    private CommonUtils() {
    }

    public static BError createError(String message, Throwable exception) {
        return ErrorCreator.createError(ModuleUtils.getModule(), "Error",
                StringUtils.fromString(message), null, null);
    }

    @SuppressWarnings("unchecked")
    public static ExecuteStatementRequest getNativeExecuteStatementRequest(
            BObject bSqlStatement, BMap<BString, Object> bConfig, DatabaseConfig databaseConfig) {
        ExecuteStatementRequest.Builder builder = ExecuteStatementRequest.builder();

        // Set the SQL statement
        ParameterizedQuery parameterizedQuery = new ParameterizedQuery(bSqlStatement);
        builder.sql(parameterizedQuery.getQueryString());
        if (parameterizedQuery.hasParameters()) {
            builder.parameters(parameterizedQuery.getParameters());
        }

        // Set the database config
        if (bConfig.containsKey(Constants.DATABASE_CONFIG)) {
            Object bDatabaseConfig = bConfig.get(Constants.DATABASE_CONFIG);
            databaseConfig = new DatabaseConfig((BMap<BString, Object>) bDatabaseConfig);
        }
        builder.clusterIdentifier(databaseConfig.clusterId()).database(databaseConfig.databaseName());
        if (databaseConfig.databaseUser() != null) {
            builder.dbUser(databaseConfig.databaseUser());
        }
        if (databaseConfig.secretArn() != null) {
            builder.secretArn(databaseConfig.secretArn());
        }

        // Set other configurations
        if (bConfig.containsKey(Constants.EXECUTE_STATEMENT_CONFIG_CLIENT_TOKEN)) {
            builder.clientToken(bConfig.getStringValue(Constants.EXECUTE_STATEMENT_CONFIG_CLIENT_TOKEN).getValue());
        }
        if (bConfig.containsKey(Constants.EXECUTE_STATEMENT_CONFIG_STATEMENT_NAME)) {
            builder.statementName(bConfig.getStringValue(
                    Constants.EXECUTE_STATEMENT_CONFIG_STATEMENT_NAME).getValue());
        }
        if (bConfig.containsKey(Constants.EXECUTE_STATEMENT_CONFIG_WITH_EVENT)) {
            builder.withEvent(bConfig.getBooleanValue(Constants.EXECUTE_STATEMENT_CONFIG_WITH_EVENT));
        }
        if (bConfig.containsKey(Constants.EXECUTE_STATEMENT_CONFIG_SESSION_ID)) {
            builder.sessionId(bConfig.getStringValue(Constants.EXECUTE_STATEMENT_CONFIG_SESSION_ID).getValue());
        }
        if (bConfig.containsKey(Constants.EXECUTE_STATEMENT_CONFIG_SESSION_KEEP_ALIVE_SECONDS)) {
            builder.sessionKeepAliveSeconds(bConfig.getIntValue(
                    Constants.EXECUTE_STATEMENT_CONFIG_SESSION_KEEP_ALIVE_SECONDS).intValue());
        }
        if (bConfig.containsKey(Constants.EXECUTE_STATEMENT_CONFIG_WORKGROUP_NAME)) {
            builder.workgroupName(bConfig.getStringValue(
                    Constants.EXECUTE_STATEMENT_CONFIG_WORKGROUP_NAME).getValue());
        }
        return builder.build();
    }

    public static BMap<BString, Object> getExecuteStatementResponse(ExecuteStatementResponse nativeResponse) {
        BMap<BString, Object> response = ValueCreator.createRecordValue(
                ModuleUtils.getModule(), Constants.EXECUTE_STATEMENT_RES_RECORD);

        BString[] dbGroups = nativeResponse.dbGroups().stream()
                .map(StringUtils::fromString)
                .toArray(BString[]::new);
        response.put(Constants.EXECUTE_STATEMENT_RES_DB_GROUPS, ValueCreator.createArrayValue(dbGroups));
        response.put(Constants.EXECUTE_STATEMENT_RES_CREATE_AT, new Utc(nativeResponse.createdAt()).build());
        response.put(Constants.EXECUTE_STATEMENT_RES_HAS_DB_GROUPS, nativeResponse.hasDbGroups());
        response.put(Constants.EXECUTE_STATEMENT_RES_STATEMENT_ID, StringUtils.fromString(nativeResponse.id()));
        if (Objects.nonNull(nativeResponse.sessionId())) {
            response.put(Constants.EXECUTE_STATEMENT_RES_SESSION_ID,
                    StringUtils.fromString(nativeResponse.sessionId()));
        }
        if (Objects.nonNull(nativeResponse.workgroupName())) {
            response.put(Constants.EXECUTE_STATEMENT_RES_WORKGROUP_NAME,
                    StringUtils.fromString(nativeResponse.workgroupName()));
        }
        return response;
    }

    @SuppressWarnings("unchecked")
    public static BatchExecuteStatementRequest getNativeBatchExecuteStatementRequest(
            BArray bSqlStatements, BMap<BString, Object> bConfig, DatabaseConfig databaseConfig) {
        BatchExecuteStatementRequest.Builder builder = BatchExecuteStatementRequest.builder();

        // Set the SQL statements
        builder.sqls(bSqlStatements.getStringArray());

        // Set the database config
        if (bConfig.containsKey(Constants.DATABASE_CONFIG)) {
            Object bDatabaseConfig = bConfig.get(Constants.DATABASE_CONFIG);
            databaseConfig = new DatabaseConfig((BMap<BString, Object>) bDatabaseConfig);
        }
        builder.clusterIdentifier(databaseConfig.clusterId()).database(databaseConfig.databaseName());
        if (databaseConfig.databaseUser() != null) {
            builder.dbUser(databaseConfig.databaseUser());
        }
        if (databaseConfig.secretArn() != null) {
            builder.secretArn(databaseConfig.secretArn());
        }

        // Set other configurations
        if (bConfig.containsKey(Constants.EXECUTE_STATEMENT_CONFIG_CLIENT_TOKEN)) {
            builder.clientToken(bConfig.getStringValue(Constants.EXECUTE_STATEMENT_CONFIG_CLIENT_TOKEN).getValue());
        }
        if (bConfig.containsKey(Constants.EXECUTE_STATEMENT_CONFIG_STATEMENT_NAME)) {
            builder.statementName(bConfig.getStringValue(
                    Constants.EXECUTE_STATEMENT_CONFIG_STATEMENT_NAME).getValue());
        }
        if (bConfig.containsKey(Constants.EXECUTE_STATEMENT_CONFIG_WITH_EVENT)) {
            builder.withEvent(bConfig.getBooleanValue(Constants.EXECUTE_STATEMENT_CONFIG_WITH_EVENT));
        }
        if (bConfig.containsKey(Constants.EXECUTE_STATEMENT_CONFIG_SESSION_ID)) {
            builder.sessionId(bConfig.getStringValue(Constants.EXECUTE_STATEMENT_CONFIG_SESSION_ID).getValue());
        }
        if (bConfig.containsKey(Constants.EXECUTE_STATEMENT_CONFIG_SESSION_KEEP_ALIVE_SECONDS)) {
            builder.sessionKeepAliveSeconds(bConfig.getIntValue(
                    Constants.EXECUTE_STATEMENT_CONFIG_SESSION_KEEP_ALIVE_SECONDS).intValue());
        }
        if (bConfig.containsKey(Constants.EXECUTE_STATEMENT_CONFIG_WORKGROUP_NAME)) {
            builder.workgroupName(bConfig.getStringValue(
                    Constants.EXECUTE_STATEMENT_CONFIG_WORKGROUP_NAME).getValue());
        }
        return builder.build();
    }

    public static BMap<BString, Object> getBatchExecuteStatementResponse(
            BatchExecuteStatementResponse nativeResponse, String[] subStatementIds) {
        BMap<BString, Object> response = ValueCreator.createRecordValue(
                ModuleUtils.getModule(), Constants.BATCH_EXECUTE_STATEMENT_RES_RECORD);

        BString[] dbGroups = nativeResponse.dbGroups().stream()
                .map(StringUtils::fromString)
                .toArray(BString[]::new);
        response.put(Constants.BATCH_EXECUTE_STATEMENT_RES_DB_GROUPS, ValueCreator.createArrayValue(dbGroups));
        response.put(Constants.BATCH_EXECUTE_STATEMENT_RES_CREATE_AT, new Utc(nativeResponse.createdAt()).build());
        response.put(Constants.BATCH_EXECUTE_STATEMENT_RES_HAS_DB_GROUPS, nativeResponse.hasDbGroups());
        response.put(Constants.BATCH_EXECUTE_STATEMENT_RES_STATEMENT_ID, StringUtils.fromString(nativeResponse.id()));
        if (Objects.nonNull(nativeResponse.sessionId())) {
            response.put(Constants.BATCH_EXECUTE_STATEMENT_RES_SESSION_ID,
                    StringUtils.fromString(nativeResponse.sessionId()));
        }
        if (Objects.nonNull(nativeResponse.workgroupName())) {
            response.put(Constants.BATCH_EXECUTE_STATEMENT_RES_WORKGROUP_NAME,
                    StringUtils.fromString(nativeResponse.workgroupName()));
        }
        BString[] bSubStatementIds = Arrays.stream(subStatementIds)
                .map(StringUtils::fromString).toArray(BString[]::new);
        response.put(Constants.BATCH_EXECUTE_STATEMENT_RES_SUB_STATEMENT_IDS,
                ValueCreator.createArrayValue(bSubStatementIds));
        return response;
    }

    public static BMap<BString, Object> getExecutionResultResponse(DescribeStatementResponse nativeResponse) {
        BMap<BString, Object> response = ValueCreator.createRecordValue(
                ModuleUtils.getModule(), Constants.EXECUTION_RESULT_RECORD);

        ArrayType subStatementDataArrayType = TypeCreator.createArrayType(ValueCreator.createRecordValue(
                ModuleUtils.getModule(), Constants.SUB_STATEMENT_DATA_RECORD).getType());
        BArray subStatementsArray = ValueCreator.createArrayValue(subStatementDataArrayType);
        for (SubStatementData subStatementData : nativeResponse.subStatements()) {
            subStatementsArray.append(getSubStatementData(subStatementData));
        }
        response.put(Constants.EXECUTION_RESULT_SUB_STATEMENTS, subStatementsArray);
        response.put(Constants.EXECUTION_RESULT_STATEMENT_ID, StringUtils.fromString(nativeResponse.id()));
        response.put(Constants.EXECUTION_RESULT_CREATED_AT, new Utc(nativeResponse.createdAt()).build());
        response.put(Constants.EXECUTION_RESULT_UPDATED_AT, new Utc(nativeResponse.updatedAt()).build());
        response.put(Constants.EXECUTION_RESULT_STATUS, StringUtils.fromString(nativeResponse.statusAsString()));
        response.put(Constants.EXECUTION_RESULT_HAS_RESULT_SET, nativeResponse.hasResultSet());
        response.put(Constants.EXECUTION_RESULT_REDSHIFT_QUERY_ID, nativeResponse.redshiftQueryId());
        response.put(Constants.EXECUTION_RESULT_RESULT_ROWS, nativeResponse.resultRows());
        response.put(Constants.EXECUTION_RESULT_RESULT_SIZE, nativeResponse.resultSize());
        response.put(Constants.EXECUTION_RESULT_HAS_QUERY_PARAMETERS, nativeResponse.hasQueryParameters());
        response.put(Constants.EXECUTION_RESULT_HAS_SUB_STATEMENTS, nativeResponse.hasSubStatements());
        response.put(Constants.EXECUTION_RESULT_REDSHIFT_PID, nativeResponse.redshiftPid());
        // Convert the duration from nanoseconds to seconds
        response.put(Constants.EXECUTION_RESULT_DURATION,
                ValueCreator.createDecimalValue(convertNanosToSeconds(nativeResponse.duration())));
        if (Objects.nonNull(nativeResponse.queryString())) {
            response.put(Constants.EXECUTION_RESULT_QUERY_STRING,
                    StringUtils.fromString(nativeResponse.queryString()));
        }
        if (Objects.nonNull(nativeResponse.sessionId())) {
            response.put(Constants.EXECUTION_RESULT_SESSION_ID, StringUtils.fromString(nativeResponse.sessionId()));
        }
        if (Objects.nonNull(nativeResponse.workgroupName())) {
            response.put(Constants.EXECUTION_RESULT_WORKGROUP_NAME,
                    StringUtils.fromString(nativeResponse.workgroupName()));
        }
        if (Objects.nonNull(nativeResponse.error()) && !nativeResponse.error().isEmpty()) {
            response.put(Constants.EXECUTION_RESULT_ERROR, StringUtils.fromString(nativeResponse.error()));
        }
        return response;
    }

    private static BMap<BString, Object> getSubStatementData(SubStatementData subStatementData) {
        BMap<BString, Object> record = ValueCreator.createRecordValue(
                ModuleUtils.getModule(), Constants.SUB_STATEMENT_DATA_RECORD);
        record.put(Constants.SUB_STATEMENT_DATA_STATEMENT_ID, StringUtils.fromString(subStatementData.id()));
        record.put(Constants.SUB_STATEMENT_DATA_CREATED_AT, new Utc(subStatementData.createdAt()).build());
        record.put(Constants.SUB_STATEMENT_DATA_UPDATED_AT, new Utc(subStatementData.updatedAt()).build());
        record.put(Constants.SUB_STATEMENT_DATA_STATUS, StringUtils.fromString(subStatementData.statusAsString()));
        record.put(Constants.SUB_STATEMENT_DATA_HAS_RESULT_SET, subStatementData.hasResultSet());
        record.put(Constants.SUB_STATEMENT_DATA_QUERY_STRING, StringUtils.fromString(subStatementData.queryString()));
        record.put(Constants.SUB_STATEMENT_DATA_REDSHIFT_QUERY_ID, subStatementData.redshiftQueryId());
        record.put(Constants.SUB_STATEMENT_DATA_RESULT_ROWS, subStatementData.resultRows());
        record.put(Constants.SUB_STATEMENT_DATA_RESULT_SIZE, subStatementData.resultSize());
        // Convert the duration from nanoseconds to seconds
        record.put(Constants.SUB_STATEMENT_DATA_DURATION,
                ValueCreator.createDecimalValue(convertNanosToSeconds(subStatementData.duration())));
        if (Objects.nonNull(subStatementData.error()) && !subStatementData.error().isEmpty()) {
            record.put(Constants.SUB_STATEMENT_DATA_ERROR, StringUtils.fromString(subStatementData.error()));
        }
        return record;
    }

    private static BigDecimal convertNanosToSeconds(long nanos) {
        return BigDecimal.valueOf(nanos).divide(BigDecimal.valueOf(1_000_000_000));
    }
}
