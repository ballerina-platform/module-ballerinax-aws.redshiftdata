//  Copyright (c) 2024, WSO2 LLC. (http://www.wso2.org).
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
import ballerina/test;

@test:Config {
    groups: ["batchExecute"]
}
isolated function testBasicBatchExecuteStatement() returns error? {
    Client redshift = check new Client(testConnectionConfig);
    string[] queries = ["SELECT * FROM Users", "SELECT * FROM Users"];
    BatchExecuteStatementResponse res = check redshift->batchExecuteStatement(queries);

    test:assertTrue(res.subStatementIds.length() == 2, "Invalid number of statement IDs");
    test:assertTrue(res.subStatementIds[0] != "", "Statement ID is empty");
    test:assertTrue(res.subStatementIds[1] != "", "Statement ID is empty");
}

@test:Config {
    groups: ["batchExecute"]
}
isolated function testBatchExecuteSessionId() returns error? {
    Client redshift = check new Client(testConnectionConfig);
    string[] queries = ["SELECT * FROM Users", "SELECT * FROM Users"];
    BatchExecuteStatementResponse res1 = check redshift->batchExecuteStatement(queries, {sessionKeepAliveSeconds: 3600});

    test:assertTrue(res1.statementId != "", "Statement ID is empty");
    test:assertTrue(res1.sessionId is string && res1.sessionId != "", "Session ID is empty");

    runtime:sleep(2); // wait for session to establish
    BatchExecuteStatementResponse res2 = check redshift->batchExecuteStatement(queries,
        {sessionId: res1.sessionId, databaseConfig: {}});
    test:assertTrue(res2.sessionId == res1.sessionId, "Session ID is not equal");
}
