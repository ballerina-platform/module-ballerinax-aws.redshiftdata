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
    groups: ["batchExecute", "liveServer"]
}
isolated function testBasicBatchExecuteStatement() returns error? {
    sql:ParameterizedQuery[] queries = [`SELECT * FROM Users`, `SELECT * FROM Users`];
    ExecutionResponse res = check redshiftData->batchExecute(queries);

    test:assertTrue(res.statementId != "");
    test:assertTrue(res.createdAt[0] > 0);
    test:assertTrue(res.sessionId is ()); // Since we are not using sessionKeepAliveSeconds
}

@test:Config {
    groups: ["batchExecute", "liveServer"]
}
isolated function testBatchExecuteSessionId() returns error? {
    ConnectionConfig connectionConfig = {
        region: awsRegion,
        authConfig,
        dbAccessConfig: {
            id: clusterId,
            database: database,
            dbUser: dbUser,
            sessionKeepAliveSeconds: 3600
        }
    };
    Client redshift = check new Client(connectionConfig);
    sql:ParameterizedQuery[] queries = [`SELECT * FROM Users`, `SELECT * FROM Users`];
    ExecutionResponse res1 = check redshift->batchExecute(queries);

    test:assertTrue(res1.statementId != "");
    test:assertTrue(res1.sessionId is string && res1.sessionId != "");

    runtime:sleep(2); // wait for session to establish
    ExecutionResponse res2 = check redshift->batchExecute(queries, {dbAccessConfig: res1.sessionId});
    test:assertTrue(res2.sessionId == res1.sessionId);
    check redshift->close();
}
