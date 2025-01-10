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

/**
 * Represents the constants related to Ballerina AWS Redshift Data Connector.
 */
public interface Constants {
    // Constants Related to Native Client Adapter
    String NATIVE_CLIENT = "nativeClient";
    String NATIVE_DB_ACCESS_CONFIG = "nativeDbAccessConfig";

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

    // Constants Related to Describe Statement Response
    String DESCRIBE_STATEMENT_RES_RECORD = "DescribeStatementResponse";
    BString DESCRIBE_STATEMENT_RES_SUB_STATEMENTS = StringUtils.fromString("subStatements");
    BString DESCRIBE_STATEMENT_RES_REDSHIFT_PID = StringUtils.fromString("redshiftPid");
    BString DESCRIBE_STATEMENT_RES_SESSION_ID = StringUtils.fromString("sessionId");

    // Constants Related to Statement Data
    String STATEMENT_DATA_RECORD = "StatementData";
    BString STATEMENT_DATA_STATEMENT_ID = StringUtils.fromString("statementId");
    BString STATEMENT_DATA_CREATED_AT = StringUtils.fromString("createdAt");
    BString STATEMENT_DATA_DURATION = StringUtils.fromString("duration");
    BString STATEMENT_DATA_ERROR = StringUtils.fromString("error");
    BString STATEMENT_DATA_HAS_RESULT_SET = StringUtils.fromString("hasResultSet");
    BString STATEMENT_DATA_QUERY_STRING = StringUtils.fromString("queryString");
    BString STATEMENT_DATA_REDSHIFT_QUERY_ID = StringUtils.fromString("redshiftQueryId");
    BString STATEMENT_DATA_RESULT_ROWS = StringUtils.fromString("resultRows");
    BString STATEMENT_DATA_RESULT_SIZE = StringUtils.fromString("resultSize");
    BString STATEMENT_DATA_STATUS = StringUtils.fromString("status");
    BString STATEMENT_DATA_UPDATED_AT = StringUtils.fromString("updatedAt");

    // Constants Related to Result Iterator
    String RESULT_ITERATOR_OBJECT = "ResultIterator";
    String RESULT_ITERATOR_RESULT_RESPONSE = "ResultResponse";
    String RESULT_ITERATOR_RECORD_TYPE = "RecordType";
    String RESULT_ITERATOR_CURRENT_RESULT_INDEX = "Index";
    String RESULT_ITERATOR_COLUMN_INDEX_MAP = "IndexMap"; // field name -> result column index
    String RESULT_ITERATOR_NATIVE_CLIENT = "nativeClient";
    String RESULT_ITERATOR_STATEMENT_ID = "statementId";

    // Constants Related to Execute Statement Config
    BString EXECUTE_STATEMENT_CONFIG_CLIENT_TOKEN = StringUtils.fromString("clientToken");
    BString EXECUTE_STATEMENT_CONFIG_STATEMENT_NAME = StringUtils.fromString("statementName");
    BString EXECUTE_STATEMENT_CONFIG_WITH_EVENT = StringUtils.fromString("withEvent");

    // Constants related to Execute Statement Response
    String EXECUTE_STATEMENT_RES_RECORD = "ExecuteStatementResponse";
    BString EXECUTE_STATEMENT_RES_CREATE_AT = StringUtils.fromString("createdAt");
    BString EXECUTE_STATEMENT_RES_DB_GROUPS = StringUtils.fromString("dbGroups");
    BString EXECUTE_STATEMENT_RES_STATEMENT_ID = StringUtils.fromString("statementId");
    BString EXECUTE_STATEMENT_RES_SESSION_ID = StringUtils.fromString("sessionId");
}
