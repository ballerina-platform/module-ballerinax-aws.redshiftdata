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
    String NATIVE_DATABASE_CONFIG = "nativeDatabaseConfig";
    BigDecimal DEFAULT_TIMEOUT = new BigDecimal(60); // In seconds
    BigDecimal DEFAULT_POLLING_INTERVAL = new BigDecimal(2); // In seconds

    // Constants Related to Connection Config
    BString REGION = StringUtils.fromString("region");
    BString AUTH_CONFIG = StringUtils.fromString("authConfig");
    BString DATABASE_CONFIG = StringUtils.fromString("databaseConfig");

    // Constants Related to Auth Config
    BString AWS_ACCESS_KEY_ID = StringUtils.fromString("accessKeyId");
    BString AWS_SECRET_ACCESS_KEY = StringUtils.fromString("secretAccessKey");
    BString AWS_SESSION_TOKEN = StringUtils.fromString("sessionToken");

    // Constants Related to Database Config
    BString CLUSTER_ID = StringUtils.fromString("clusterId");
    BString DATABASE_NAME = StringUtils.fromString("databaseName");
    BString DATABASE_USER = StringUtils.fromString("databaseUser");
    BString SECRET_ARN = StringUtils.fromString("secretArn");

    // Constants Related to Parameterized Query
    BString QUERY_STRINGS = StringUtils.fromString("strings");
    BString QUERY_INSERTIONS = StringUtils.fromString("insertions");

    // Constants Related to Result Config
    BString TIMEOUT = StringUtils.fromString("timeout");
    BString POLLING_INTERVAL = StringUtils.fromString("pollingInterval");

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
}
