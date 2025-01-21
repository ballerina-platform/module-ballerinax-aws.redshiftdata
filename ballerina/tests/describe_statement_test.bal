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
    groups: ["describeStatement"]
}
isolated function testBasicDescribeStatement() returns error? {
    Client redshift = check new Client(testConnectionConfig);

    sql:ParameterizedQuery query = `SELECT * FROM Users;`;
    ExecutionResponse executionResponse = check redshift->executeStatement(query);
    DescribeStatementResponse describeStatementResponse =
        check waitForDescribeStatementCompletion(redshift, executionResponse.statementId);

    test:assertTrue(describeStatementResponse.statementId != "", "Statement ID is empty");
    test:assertTrue(describeStatementResponse.createdAt[0] > 0, "Invalid createdAt time");
    test:assertTrue(describeStatementResponse.duration > 0d, "Invalid duration");
    test:assertTrue(describeStatementResponse.redshiftPid > 0, "Invalid redshiftPid");
    test:assertTrue(describeStatementResponse.sessionId is (), "Session ID is not nil");
    test:assertTrue(describeStatementResponse.subStatements is (), "Invalid subStatements count");
    test:assertEquals(describeStatementResponse.hasResultSet, true, "Invalid hasResultSet value");
    test:assertEquals(describeStatementResponse.queryString, query.strings[0], "Invalid query string");
    test:assertEquals(describeStatementResponse.statementId, executionResponse.statementId,
            "Statement ID mismatch");
    check redshift->close();
}

@test:Config {
    enable: IS_TESTS_ENABLED,
    groups: ["describeStatement"]
}
isolated function testBatchDescribeStatement() returns error? {
    Client redshift = check new Client(testConnectionConfig);

    sql:ParameterizedQuery[] queries = [`SELECT * FROM Users`, `SELECT * FROM Users;`];
    ExecutionResponse res = check redshift->batchExecuteStatement(queries);
    DescribeStatementResponse describeStatementResponse =
        check waitForDescribeStatementCompletion(redshift, res.statementId);

    test:assertTrue(describeStatementResponse.redshiftPid > 0, "Invalid redshiftPid");
    test:assertTrue(describeStatementResponse.sessionId is (), "Session ID is not nil");
    test:assertTrue(describeStatementResponse.subStatements !is (), "Invalid subStatements count");
    StatementData[] subStatements = describeStatementResponse.subStatements ?: [];
    test:assertEquals(subStatements.length(), 2, "Invalid subStatements count");

    test:assertTrue(describeStatementResponse.statementId != "", "Statement ID is empty");
    test:assertTrue(describeStatementResponse.createdAt[0] > 0, "Invalid createdAt time");
    test:assertTrue(describeStatementResponse.duration > 0d, "Invalid duration");
    test:assertTrue(describeStatementResponse.queryString is (), "Invalid query string");
    test:assertTrue(describeStatementResponse.'error is (), "Error is not nil");
    test:assertTrue(describeStatementResponse.redshiftPid > 0, "Invalid redshiftPid");
    test:assertTrue(describeStatementResponse.updatedAt[0] > 0, "Invalid updatedAt time");
    test:assertEquals(describeStatementResponse.status, FINISHED, "Invalid status");
    test:assertEquals(describeStatementResponse.hasResultSet, true, "Invalid hasResultSet value");
    test:assertEquals(describeStatementResponse.redshiftQueryId, 0, "Invalid redshiftQueryId");
    test:assertEquals(describeStatementResponse.resultRows, -1, "Invalid resultRows");
    test:assertEquals(describeStatementResponse.resultSize, -1, "Invalid resultSize");

    StatementData subStatement1 = subStatements[0];
    test:assertTrue(subStatement1.statementId != "", "SubStatement: Statement ID is empty");
    test:assertTrue(subStatement1.createdAt[0] > 0, "SubStatement: Invalid createdAt time");
    test:assertTrue(subStatement1.duration > 0d, "SubStatement: Invalid duration");
    test:assertTrue(subStatement1.'error is (), "SubStatement: Error is not nil");
    test:assertTrue(subStatement1.redshiftQueryId > 0, "SubStatement: Invalid redshiftQueryId");
    test:assertTrue(subStatement1.resultRows > 0, "SubStatement: Invalid resultRows");
    test:assertTrue(subStatement1.resultSize > 0, "SubStatement: Invalid resultSize");
    test:assertEquals(subStatement1.hasResultSet, true, "SubStatement: Invalid hasResultSet value");
    test:assertEquals(subStatement1.status, FINISHED, "SubStatement: Invalid status");
    test:assertEquals(subStatement1.queryString, "SELECT * FROM Users", "SubStatement: Invalid query string");
    check redshift->close();
}

@test:Config {
    enable: IS_TESTS_ENABLED,
    groups: ["describeStatement"]
}
isolated function testIncorrectStatementDescribeStatement() returns error? {
    Client redshift = check new Client(testConnectionConfig);
    ExecutionResponse executionResponse = check redshift->executeStatement(`SELECT * FROM non_existent_table;`);
    DescribeStatementResponse describeStatementResponse =
        check waitForDescribeStatementCompletion(redshift, executionResponse.statementId);

    test:assertEquals(describeStatementResponse.status, FAILED, "Invalid status");
    test:assertTrue(describeStatementResponse.'error is string, "Error message is nil");
    test:assertTrue(describeStatementResponse.'error != "", "Error message is empty");
    check redshift->close();
}

@test:Config {
    enable: IS_TESTS_ENABLED,
    groups: ["describeStatement"]
}
isolated function testIncorrectBatchStatementDescribeStatement() returns error? {
    Client redshift = check new Client(testConnectionConfig);
    sql:ParameterizedQuery[] queries = [`SELECT * FROM Users`, `SELECT * FROM non_existent_table;`];
    ExecutionResponse res = check redshift->batchExecuteStatement(queries);
    DescribeStatementResponse describeStatementResponse =
        check waitForDescribeStatementCompletion(redshift, res.statementId);

    test:assertEquals(describeStatementResponse.status, FAILED, "Invalid status");
    test:assertTrue(describeStatementResponse.'error is string, "Error message is nil");
    test:assertTrue(describeStatementResponse.'error != "", "Error message is empty");

    StatementData[] subStatements = describeStatementResponse.subStatements ?: [];
    test:assertEquals(subStatements.length(), 2, "Invalid subStatements count");
    test:assertEquals(subStatements[0].status, FINISHED, "SubStatement 1: Invalid status");
    test:assertTrue(subStatements[0].'error is (), "SubStatement 1: Error is not nil");

    test:assertEquals(subStatements[1].status, FAILED, "SubStatement 2: Invalid status");
    test:assertTrue(subStatements[1].'error is string, "SubStatement 2: Error message is nil");
    test:assertTrue(subStatements[1].'error != "", "SubStatement 2: Error message is empty");
    check redshift->close();
}

@test:Config {
    enable: IS_TESTS_ENABLED,
    groups: ["describeStatement"]
}
isolated function testDescribeStatementWithInvalidStatementId() returns error? {
    Client redshift = check new Client(testConnectionConfig);
    StatementId invalidStatementId = "InvalidStatementId";
    DescribeStatementResponse|Error res = redshift->describeStatement(invalidStatementId);
    test:assertTrue(res is Error, "Query result is not an error");
    if (res is Error) {
        test:assertEquals(res.message(), "Statement ID validation failed: Validation failed for " +
                "'$:pattern' constraint(s).", "Invalid Error Message");
    }
    check redshift->close();
}

// Helper function
isolated function waitForDescribeStatementCompletion(Client redshift, string statementId) returns DescribeStatementResponse|Error {
    int i = 0;
    while (i < 10) {
        DescribeStatementResponse|Error describeStatementResponse = redshift->describeStatement(statementId);
        if (describeStatementResponse is Error) {
            return describeStatementResponse;
        }
        match describeStatementResponse.status {
            "FINISHED"|"FAILED"|"ABORTED" => {
                return describeStatementResponse;
            }
        }
        i = i + 1;
        runtime:sleep(1);
    }
    panic error("Statement execution did not finish within the expected time");
}
