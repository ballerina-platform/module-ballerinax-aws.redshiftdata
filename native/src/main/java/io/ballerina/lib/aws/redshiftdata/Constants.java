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

import io.ballerina.runtime.api.utils.StringUtils;
import io.ballerina.runtime.api.values.BString;

import java.math.BigDecimal;

/**
 * Represents the constants related to Ballerina AWS Redshift Data Connector.
 */
public interface Constants {
    // Constants Related to Native Client Adapter
    String NATIVE_CLIENT = "nativeClient";
    String NATIVE_DB_ACCESS_CONFIG = "nativeDbAccessConfig";
    BigDecimal DEFAULT_TIMEOUT = new BigDecimal(60); // In seconds
    BigDecimal DEFAULT_POLLING_INTERVAL = new BigDecimal(2); // In seconds

    // Constants Related to Connection Config
    BString CONNECTION_CONFIG_REGION = StringUtils.fromString("region");
    BString CONNECTION_CONFIG_AUTH_CONFIG = StringUtils.fromString("authConfig");
    BString CONNECTION_CONFIG_DB_ACCESS_CONFIG = StringUtils.fromString("dbAccessConfig");

    // Constants Related to Auth Config
    BString AWS_ACCESS_KEY_ID = StringUtils.fromString("accessKeyId");
    BString AWS_SECRET_ACCESS_KEY = StringUtils.fromString("secretAccessKey");
    BString AWS_SESSION_TOKEN = StringUtils.fromString("sessionToken");

    // Constants Related to Cluster
    BString CLUSTER_ID = StringUtils.fromString("id");
    BString CLUSTER_DATABASE = StringUtils.fromString("database");
    BString CLUSTER_DB_USER = StringUtils.fromString("dbUser");
    BString CLUSTER_SECRET_ARN = StringUtils.fromString("secretArn");
    BString CLUSTER_SESSION_KEEP_ALIVE_SECONDS = StringUtils.fromString("sessionKeepAliveSeconds");

    // Constants Related to WorkGroup
    BString WORK_GROUP_NAME = StringUtils.fromString("name");
    BString WORK_GROUP_DATABASE = StringUtils.fromString("database");
    BString WORK_GROUP_SECRET_ARN = StringUtils.fromString("secretArn");
    BString WORK_GROUP_SESSION_KEEP_ALIVE_SECONDS = StringUtils.fromString("sessionKeepAliveSeconds");

    // Constants Related to Parameterized Query
    BString QUERY_STRINGS = StringUtils.fromString("strings");
    BString QUERY_INSERTIONS = StringUtils.fromString("insertions");

    // Constants Related to Result Config
    BString RESULT_CONFIG_TIMEOUT = StringUtils.fromString("timeout");
    BString RESULT_CONFIG_POLLING_INTERVAL = StringUtils.fromString("pollingInterval");

    // Constants Related to Execution Result
    String EXECUTION_RESULT_RECORD = "ExecutionResult";
    BString EXECUTION_RESULT_STATEMENT_ID = StringUtils.fromString("statementId");
    BString EXECUTION_RESULT_CREATED_AT = StringUtils.fromString("createdAt");
    BString EXECUTION_RESULT_DURATION = StringUtils.fromString("duration");
    BString EXECUTION_RESULT_ERROR = StringUtils.fromString("error");
    BString EXECUTION_RESULT_HAS_RESULT_SET = StringUtils.fromString("hasResultSet");
    BString EXECUTION_RESULT_QUERY_STRING = StringUtils.fromString("queryString");
    BString EXECUTION_RESULT_REDSHIFT_QUERY_ID = StringUtils.fromString("redshiftQueryId");
    BString EXECUTION_RESULT_RESULT_ROWS = StringUtils.fromString("resultRows");
    BString EXECUTION_RESULT_RESULT_SIZE = StringUtils.fromString("resultSize");
    BString EXECUTION_RESULT_STATUS = StringUtils.fromString("status");
    BString EXECUTION_RESULT_UPDATED_AT = StringUtils.fromString("updatedAt");
    BString EXECUTION_RESULT_HAS_QUERY_PARAMETERS = StringUtils.fromString("hasQueryParameters");
    BString EXECUTION_RESULT_HAS_SUB_STATEMENTS = StringUtils.fromString("hasSubStatements");
    BString EXECUTION_RESULT_REDSHIFT_PID = StringUtils.fromString("redshiftPid");
    BString EXECUTION_RESULT_SESSION_ID = StringUtils.fromString("sessionId");
    BString EXECUTION_RESULT_SUB_STATEMENTS = StringUtils.fromString("subStatements");
    BString EXECUTION_RESULT_WORKGROUP_NAME = StringUtils.fromString("workgroupName");

    // Constants Related to Sub Statement Data
    String SUB_STATEMENT_DATA_RECORD = "SubStatementData";
    BString SUB_STATEMENT_DATA_STATEMENT_ID = StringUtils.fromString("statementId");
    BString SUB_STATEMENT_DATA_CREATED_AT = StringUtils.fromString("createdAt");
    BString SUB_STATEMENT_DATA_DURATION = StringUtils.fromString("duration");
    BString SUB_STATEMENT_DATA_ERROR = StringUtils.fromString("error");
    BString SUB_STATEMENT_DATA_HAS_RESULT_SET = StringUtils.fromString("hasResultSet");
    BString SUB_STATEMENT_DATA_QUERY_STRING = StringUtils.fromString("queryString");
    BString SUB_STATEMENT_DATA_REDSHIFT_QUERY_ID = StringUtils.fromString("redshiftQueryId");
    BString SUB_STATEMENT_DATA_RESULT_ROWS = StringUtils.fromString("resultRows");
    BString SUB_STATEMENT_DATA_RESULT_SIZE = StringUtils.fromString("resultSize");
    BString SUB_STATEMENT_DATA_STATUS = StringUtils.fromString("status");
    BString SUB_STATEMENT_DATA_UPDATED_AT = StringUtils.fromString("updatedAt");

    // Constants Related to Result Iterator
    String RESULT_ITERATOR_OBJECT = "ResultIterator";
    String NATIVE_COLUMN_METADATA = "ColumnMetadata";
    String NATIVE_RESULT_RESPONSE = "ResultResponse";
    String NATIVE_INNER_RESULT_ITERATOR = "InnerGetResultIterator";
    String NATIVE_RECORD_TYPE = "RecordType";
    String NATIVE_INDEX = "Index";
    String NATIVE_COLUMN_INDEX_MAP = "IndexMap"; // field name -> result column index

    // Constants Related to Execute Statement Config
    BString EXECUTE_STATEMENT_CONFIG_CLIENT_TOKEN = StringUtils.fromString("clientToken");
    BString EXECUTE_STATEMENT_CONFIG_STATEMENT_NAME = StringUtils.fromString("statementName");
    BString EXECUTE_STATEMENT_CONFIG_WITH_EVENT = StringUtils.fromString("withEvent");
    BString EXECUTE_STATEMENT_CONFIG_SESSION_ID = StringUtils.fromString("sessionId");
    BString EXECUTE_STATEMENT_CONFIG_SESSION_KEEP_ALIVE_SECONDS = StringUtils.fromString("sessionKeepAliveSeconds");
    BString EXECUTE_STATEMENT_CONFIG_WORKGROUP_NAME = StringUtils.fromString("workgroupName");

    // Constants related to Execute Statement Response
    String EXECUTE_STATEMENT_RES_RECORD = "ExecuteStatementResponse";
    BString EXECUTE_STATEMENT_RES_CREATE_AT = StringUtils.fromString("createdAt");
    BString EXECUTE_STATEMENT_RES_HAS_DB_GROUPS = StringUtils.fromString("hasDbGroups");
    BString EXECUTE_STATEMENT_RES_DB_GROUPS = StringUtils.fromString("dbGroups");
    BString EXECUTE_STATEMENT_RES_STATEMENT_ID = StringUtils.fromString("statementId");
    BString EXECUTE_STATEMENT_RES_SESSION_ID = StringUtils.fromString("sessionId");
    BString EXECUTE_STATEMENT_RES_WORKGROUP_NAME = StringUtils.fromString("workgroupName");

    // Constants related to Batch Execute Statement Response
    String BATCH_EXECUTE_STATEMENT_RES_RECORD = "BatchExecuteStatementResponse";
    BString BATCH_EXECUTE_STATEMENT_RES_SUB_STATEMENT_IDS = StringUtils.fromString("subStatementIds");
    BString BATCH_EXECUTE_STATEMENT_RES_CREATE_AT = StringUtils.fromString("createdAt");
    BString BATCH_EXECUTE_STATEMENT_RES_HAS_DB_GROUPS = StringUtils.fromString("hasDbGroups");
    BString BATCH_EXECUTE_STATEMENT_RES_DB_GROUPS = StringUtils.fromString("dbGroups");
    BString BATCH_EXECUTE_STATEMENT_RES_STATEMENT_ID = StringUtils.fromString("statementId");
    BString BATCH_EXECUTE_STATEMENT_RES_SESSION_ID = StringUtils.fromString("sessionId");
    BString BATCH_EXECUTE_STATEMENT_RES_WORKGROUP_NAME = StringUtils.fromString("workgroupName");
}
