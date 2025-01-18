//  Copyright (c) 2025, WSO2 LLC. (http://www.wso2.org).
//
//  WSO2 LLC. licenses this file to you under the Apache License,
//  Version 2.0 (the "License"); you may not use this file except
//  in compliance with the License.
//  You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
//  Unless required by applicable law or agreed to in writing,
//  software distributed under the License is distributed on an
//  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
//  KIND, either express or implied. See the License for the
//  specific language governing permissions and limitations
//  under the License.

import ballerina/lang.runtime;
import ballerina/sql;
import ballerina/test;

@test:Config {
    enable: IS_TESTS_ENABLED,
    groups: ["batchExecute"]
}
isolated function testBasicBatchExecuteStatement() returns error? {
    Client redshift = check new Client(testConnectionConfig);
    sql:ParameterizedQuery[] queries = [`SELECT * FROM Users`, `SELECT * FROM Users`];
    ExecuteStatementResponse res = check redshift->batchExecuteStatement(queries);

    test:assertTrue(res.statementId != "", "Statement ID is empty");
    test:assertTrue(res.createdAt[0] > 0, "Invalid createdAt time");
    test:assertTrue(res.sessionId is (), "Session ID is not nil"); // Since we are not using sessionKeepAliveSeconds
    check redshift->close();
}

@test:Config {
    enable: IS_TESTS_ENABLED,
    groups: ["batchExecute"]
}
isolated function testBatchExecuteSessionId() returns error? {
    ConnectionConfig connectionConfig = {
        region: testRegion,
        authConfig: testAuthConfig,
        dbAccessConfig: {
            id: TEST_CLUSTER_ID,
            database: TEST_DATABASE_NAME,
            dbUser: TEST_DB_USER,
            sessionKeepAliveSeconds: 3600
        }
    };
    Client redshift = check new Client(connectionConfig);
    sql:ParameterizedQuery[] queries = [`SELECT * FROM Users`, `SELECT * FROM Users`];
    ExecuteStatementResponse res1 = check redshift->batchExecuteStatement(queries);

    test:assertTrue(res1.statementId != "", "Statement ID is empty");
    test:assertTrue(res1.sessionId is string && res1.sessionId != "", "Session ID is empty");

    runtime:sleep(2); // wait for session to establish
    ExecuteStatementResponse res2 = check redshift->batchExecuteStatement(queries,
        {dbAccessConfig: res1.sessionId});
    test:assertTrue(res2.sessionId == res1.sessionId, "Session ID is not equal");
    check redshift->close();
}
