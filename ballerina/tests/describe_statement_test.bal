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
    enable: isTestsEnabled,
    groups: ["describeStatement"]
}
isolated function testBasicDescribeStatement() returns error? {
    Client redshift = check new Client(testConnectionConfig);

    sql:ParameterizedQuery query = `SELECT * FROM Users;`;
    ExecutionResponse executionResponse = check redshift->executeStatement(query);
    DescriptionResponse descriptionResponse =
        check waitForDescribeStatementCompletion(redshift, executionResponse.statementId);

    test:assertTrue(descriptionResponse.statementId != "", "Statement ID is empty");
    test:assertTrue(descriptionResponse.createdAt[0] > 0, "Invalid createdAt time");
    test:assertTrue(descriptionResponse.duration > 0d, "Invalid duration");
    test:assertTrue(descriptionResponse.redshiftPid > 0, "Invalid redshiftPid");
    test:assertTrue(descriptionResponse.sessionId is (), "Session ID is not nil");
    test:assertTrue(descriptionResponse.subStatements is (), "Invalid subStatements count");
    test:assertEquals(descriptionResponse.hasResultSet, true, "Invalid hasResultSet value");
    test:assertEquals(descriptionResponse.queryString, query.strings[0], "Invalid query string");
    test:assertEquals(descriptionResponse.statementId, executionResponse.statementId,
            "Statement ID mismatch");
    check redshift->close();
}

@test:Config {
    enable: isTestsEnabled,
    groups: ["describeStatement"]
}
isolated function testBatchDescribeStatement() returns error? {
    Client redshift = check new Client(testConnectionConfig);

    sql:ParameterizedQuery[] queries = [`SELECT * FROM Users`, `SELECT * FROM Users;`];
    ExecutionResponse res = check redshift->batchExecuteStatement(queries);
    DescriptionResponse descriptionResponse =
        check waitForDescribeStatementCompletion(redshift, res.statementId);

    test:assertTrue(descriptionResponse.redshiftPid > 0, "Invalid redshiftPid");
    test:assertTrue(descriptionResponse.sessionId is (), "Session ID is not nil");
    test:assertTrue(descriptionResponse.subStatements !is (), "Invalid subStatements count");
    StatementData[] subStatements = descriptionResponse.subStatements ?: [];
    test:assertEquals(subStatements.length(), 2, "Invalid subStatements count");

    test:assertTrue(descriptionResponse.statementId != "", "Statement ID is empty");
    test:assertTrue(descriptionResponse.createdAt[0] > 0, "Invalid createdAt time");
    test:assertTrue(descriptionResponse.duration > 0d, "Invalid duration");
    test:assertTrue(descriptionResponse.queryString is (), "Invalid query string");
    test:assertTrue(descriptionResponse.'error is (), "Error is not nil");
    test:assertTrue(descriptionResponse.redshiftPid > 0, "Invalid redshiftPid");
    test:assertTrue(descriptionResponse.updatedAt[0] > 0, "Invalid updatedAt time");
    test:assertEquals(descriptionResponse.status, FINISHED, "Invalid status");
    test:assertEquals(descriptionResponse.hasResultSet, true, "Invalid hasResultSet value");
    test:assertEquals(descriptionResponse.redshiftQueryId, 0, "Invalid redshiftQueryId");
    test:assertEquals(descriptionResponse.resultRows, -1, "Invalid resultRows");
    test:assertEquals(descriptionResponse.resultSize, -1, "Invalid resultSize");

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
    enable: isTestsEnabled,
    groups: ["describeStatement"]
}
isolated function testIncorrectStatementDescribeStatement() returns error? {
    Client redshift = check new Client(testConnectionConfig);
    ExecutionResponse executionResponse = check redshift->executeStatement(`SELECT * FROM non_existent_table;`);
    DescriptionResponse descriptionResponse =
        check waitForDescribeStatementCompletion(redshift, executionResponse.statementId);

    test:assertEquals(descriptionResponse.status, FAILED, "Invalid status");
    test:assertTrue(descriptionResponse.'error is string, "Error message is nil");
    test:assertTrue(descriptionResponse.'error != "", "Error message is empty");
    check redshift->close();
}

@test:Config {
    enable: isTestsEnabled,
    groups: ["describeStatement"]
}
isolated function testIncorrectBatchStatementDescribeStatement() returns error? {
    Client redshift = check new Client(testConnectionConfig);
    sql:ParameterizedQuery[] queries = [`SELECT * FROM Users`, `SELECT * FROM non_existent_table;`];
    ExecutionResponse res = check redshift->batchExecuteStatement(queries);
    DescriptionResponse descriptionResponse =
        check waitForDescribeStatementCompletion(redshift, res.statementId);

    test:assertEquals(descriptionResponse.status, FAILED, "Invalid status");
    test:assertTrue(descriptionResponse.'error is string, "Error message is nil");
    test:assertTrue(descriptionResponse.'error != "", "Error message is empty");

    StatementData[] subStatements = descriptionResponse.subStatements ?: [];
    test:assertEquals(subStatements.length(), 2, "Invalid subStatements count");
    test:assertEquals(subStatements[0].status, FINISHED, "SubStatement 1: Invalid status");
    test:assertTrue(subStatements[0].'error is (), "SubStatement 1: Error is not nil");

    test:assertEquals(subStatements[1].status, FAILED, "SubStatement 2: Invalid status");
    test:assertTrue(subStatements[1].'error is string, "SubStatement 2: Error message is nil");
    test:assertTrue(subStatements[1].'error != "", "SubStatement 2: Error message is empty");
    check redshift->close();
}

@test:Config {
    enable: isTestsEnabled,
    groups: ["describeStatement"]
}
isolated function testDescribeStatementWithInvalidStatementId() returns error? {
    Client redshift = check new Client(testConnectionConfig);
    StatementId invalidStatementId = "InvalidStatementId";
    DescriptionResponse|Error res = redshift->describeStatement(invalidStatementId);
    test:assertTrue(res is Error, "Query result is not an error");
    if res is Error {
        test:assertEquals(res.message(), "Invalid statement ID format.", "Invalid Error Message");
    }
    check redshift->close();
}

// Helper function
isolated function waitForDescribeStatementCompletion(Client redshift, string statementId) returns DescriptionResponse|Error {
    int i = 0;
    while i < 10 {
        DescriptionResponse|Error descriptionResponse = redshift->describeStatement(statementId);
        if descriptionResponse is Error {
            return descriptionResponse;
        }
        match descriptionResponse.status {
            "FINISHED"|"FAILED"|"ABORTED" => {
                return descriptionResponse;
            }
        }
        i = i + 1;
        runtime:sleep(1);
    }
    panic error("Statement execution did not finish within the expected time");
}
