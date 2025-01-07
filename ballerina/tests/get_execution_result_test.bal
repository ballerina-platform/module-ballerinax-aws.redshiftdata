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

import ballerina/sql;
import ballerina/test;
import ballerina/time;

@test:Config {
    groups: ["getExecutionResult"]
}
isolated function testBasicExecutionResult() returns error? {
    Client redshift = check new Client(testConnectionConfig);

    sql:ParameterizedQuery query = `SELECT * FROM Users;`;
    time:Utc startTime = time:utcNow();
    ExecuteStatementResponse res = check redshift->executeStatement(query);
    time:Utc endTime = time:utcNow();

    ExecutionResult executionResult = check redshift->getExecutionResult(res.statementId);

    test:assertTrue(executionResult.statementId != "", "Statement ID is empty");
    test:assertTrue(executionResult.statementId == res.statementId, "Statement ID mismatch");
    test:assertTrue(executionResult.createdAt[0] >= startTime[0] && res.createdAt[0] <= endTime[0],
            "Invalid createdAt time");
    test:assertTrue(executionResult.duration > 0d, "Invalid duration");
    test:assertTrue(executionResult.hasResultSet == true, "Invalid hasResultSet value");
    test:assertTrue(executionResult.queryString == query.strings[0], "Invalid query string");
    test:assertTrue(executionResult.redshiftPid > 0, "Invalid redshiftPid");
    test:assertTrue(executionResult.workgroupName is (), "Workgroup name is not nill");
    test:assertTrue(executionResult.sessionId is (), "Session ID is not nill");
    test:assertTrue(executionResult.subStatements.length() == 0, "Invalid subStatements count");
}

@test:Config {
    groups: ["getExecutionResult"]
}
isolated function testBatchExecutionResult() returns error? {
    Client redshift = check new Client(testConnectionConfig);

    time:Utc startTime = time:utcNow();
    string[] queries = ["SELECT * FROM Users", "SELECT * FROM Users;"];
    BatchExecuteStatementResponse res = check redshift->batchExecuteStatement(queries);
    time:Utc endTime = time:utcNow();

    ExecutionResult executionResult = check redshift->getExecutionResult(res.statementId);
    SubStatementData subStatement1 = executionResult.subStatements[0];

    test:assertTrue(subStatement1.statementId != "", "Statement ID is empty");
    test:assertTrue(subStatement1.createdAt[0] >= startTime[0] && res.createdAt[0] <= endTime[0],
            "Invalid createdAt time");
    test:assertTrue(subStatement1.duration > 0d, "Invalid duration");
    test:assertTrue(subStatement1.'error is (), "Error is not nill");
    test:assertTrue(subStatement1.hasResultSet == true, "Invalid hasResultSet value");
    test:assertTrue(subStatement1.queryString == queries[0], "Invalid query string");
    test:assertTrue(subStatement1.redshiftQueryId > 0, "Invalid redshiftQueryId");
    test:assertTrue(subStatement1.resultRows > 0, "Invalid resultRows");
    test:assertTrue(subStatement1.resultSize > 0, "Invalid resultSize");
    test:assertTrue(subStatement1.status == FINISHED, "Invalid status");
    test:assertTrue(subStatement1.updatedAt[0] >= subStatement1.createdAt[0], "Invalid updatedAt time");
}

@test:Config {
    groups: ["getExecutionResult"]
}
isolated function testIncorrectStatementExecutionResult() returns error? {
    Client redshift = check new Client(testConnectionConfig);
    ExecuteStatementResponse res = check redshift->executeStatement(`SELECT * FROM non_existent_table;`);
    ExecutionResult|Error executionResult = redshift->getExecutionResult(res.statementId);
    test:assertTrue(executionResult is Error, "Execution result is not an error");
}

@test:Config {
    groups: ["getExecutionResult"]
}
isolated function testIncorrectBatchStatementExecutionResult() returns error? {
    Client redshift = check new Client(testConnectionConfig);
    string[] queries = ["SELECT * FROM Users", "SELECT * FROM non_existent_table;"];
    BatchExecuteStatementResponse res = check redshift->batchExecuteStatement(queries);
    ExecutionResult|Error executionResult = redshift->getExecutionResult(res.statementId);
    test:assertTrue(executionResult is Error, "Execution result is not an error");
}

@test:Config {
    groups: ["getExecutionResult"]
}
isolated function testExecutionResultWithConfig() returns error? {
    // This query will take about 15 seconds to execute
    sql:ParameterizedQuery query =
        `SELECT SUM(SQRT(ABS(SIN(a.num * b.num * random())))) AS expensive_computation
            FROM generate_series(1, 10000) AS a(num)
            CROSS JOIN generate_series(1, 2000) AS b(num);
        `;
    Client redshift = check new Client(testConnectionConfig);
    ExecuteStatementResponse res = check redshift->executeStatement(query);

    RetrieveResultConfig retrieveResultConfig = {
        timeout: 5,
        pollingInterval: 2
    };
    ExecutionResult|Error executionResult = redshift->getExecutionResult(
        res.statementId, retrieveResultConfig = retrieveResultConfig);
    test:assertTrue(executionResult is Error, "Result stream is not an error");
    if (executionResult is Error) {
        test:assertTrue(executionResult.message().includes("Statement execution timed out"), "Invalid error message");
    }
}
