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
    groups: ["describeStatement"]
}
isolated function testBasicDescribeStatement() returns error? {
    Client redshift = check new Client(testConnectionConfig);

    sql:ParameterizedQuery query = `SELECT * FROM Users;`;
    ExecutionResponse executionResponse = check redshift->executeStatement(query);
    DescriptionResponse descriptionResponse =
        check waitForDescribeStatementCompletion(redshift, executionResponse.statementId);

    test:assertTrue(descriptionResponse.statementId != "");
    test:assertTrue(descriptionResponse.createdAt[0] > 0);
    test:assertTrue(descriptionResponse.duration > 0d);
    test:assertTrue(descriptionResponse.redshiftPid > 0);
    test:assertTrue(descriptionResponse.sessionId is ());
    test:assertTrue(descriptionResponse.subStatements is ());
    test:assertEquals(descriptionResponse.hasResultSet, true);
    test:assertEquals(descriptionResponse.queryString, query.strings[0]);
    test:assertEquals(descriptionResponse.statementId, executionResponse.statementId);
    check redshift->close();
}

@test:Config {
    groups: ["describeStatement"]
}
isolated function testBatchDescribeStatement() returns error? {
    Client redshift = check new Client(testConnectionConfig);

    sql:ParameterizedQuery[] queries = [`SELECT * FROM Users`, `SELECT * FROM Users;`];
    ExecutionResponse res = check redshift->batchExecuteStatement(queries);
    DescriptionResponse descriptionResponse =
        check waitForDescribeStatementCompletion(redshift, res.statementId);

    test:assertTrue(descriptionResponse.redshiftPid > 0);
    test:assertTrue(descriptionResponse.sessionId is ());
    test:assertTrue(descriptionResponse.subStatements !is ());
    StatementData[] subStatements = descriptionResponse.subStatements ?: [];
    test:assertEquals(subStatements.length(), 2);

    test:assertTrue(descriptionResponse.statementId != "");
    test:assertTrue(descriptionResponse.createdAt[0] > 0);
    test:assertTrue(descriptionResponse.duration > 0d);
    test:assertTrue(descriptionResponse.queryString is ());
    test:assertTrue(descriptionResponse.'error is ());
    test:assertTrue(descriptionResponse.redshiftPid > 0);
    test:assertTrue(descriptionResponse.updatedAt[0] > 0);
    test:assertEquals(descriptionResponse.status, FINISHED);
    test:assertEquals(descriptionResponse.hasResultSet, true);
    test:assertEquals(descriptionResponse.redshiftQueryId, 0);
    test:assertEquals(descriptionResponse.resultRows, -1);
    test:assertEquals(descriptionResponse.resultSize, -1);

    StatementData subStatement1 = subStatements[0];
    test:assertTrue(subStatement1.statementId != "");
    test:assertTrue(subStatement1.createdAt[0] > 0);
    test:assertTrue(subStatement1.duration > 0d);
    test:assertTrue(subStatement1.'error is ());
    test:assertTrue(subStatement1.redshiftQueryId > 0);
    test:assertTrue(subStatement1.resultRows > 0);
    test:assertTrue(subStatement1.resultSize > 0);
    test:assertEquals(subStatement1.hasResultSet, true);
    test:assertEquals(subStatement1.status, FINISHED);
    test:assertEquals(subStatement1.queryString, "SELECT * FROM Users");
    check redshift->close();
}

@test:Config {
    groups: ["describeStatement"]
}
isolated function testIncorrectStatementDescribeStatement() returns error? {
    Client redshift = check new Client(testConnectionConfig);
    ExecutionResponse executionResponse = check redshift->executeStatement(`SELECT * FROM non_existent_table;`);
    DescriptionResponse descriptionResponse =
        check waitForDescribeStatementCompletion(redshift, executionResponse.statementId);

    test:assertEquals(descriptionResponse.status, FAILED);
    test:assertTrue(descriptionResponse.'error is string);
    test:assertTrue(descriptionResponse.'error != "");
    check redshift->close();
}

@test:Config {
    groups: ["describeStatement"]
}
isolated function testIncorrectBatchStatementDescribeStatement() returns error? {
    Client redshift = check new Client(testConnectionConfig);
    sql:ParameterizedQuery[] queries = [`SELECT * FROM Users`, `SELECT * FROM non_existent_table;`];
    ExecutionResponse res = check redshift->batchExecuteStatement(queries);
    DescriptionResponse descriptionResponse =
        check waitForDescribeStatementCompletion(redshift, res.statementId);

    test:assertEquals(descriptionResponse.status, FAILED);
    test:assertTrue(descriptionResponse.'error is string);
    test:assertTrue(descriptionResponse.'error != "");

    StatementData[] subStatements = descriptionResponse.subStatements ?: [];
    test:assertEquals(subStatements.length(), 2);
    test:assertEquals(subStatements[0].status, FINISHED);
    test:assertTrue(subStatements[0].'error is ());

    test:assertEquals(subStatements[1].status, FAILED);
    test:assertTrue(subStatements[1].'error is string);
    test:assertTrue(subStatements[1].'error != "");
    check redshift->close();
}

@test:Config {
    groups: ["describeStatement"]
}
isolated function testDescribeStatementWithInvalidStatementId() returns error? {
    Client redshift = check new Client(testConnectionConfig);
    StatementId invalidStatementId = "InvalidStatementId";
    DescriptionResponse|Error res = redshift->describeStatement(invalidStatementId);
    test:assertTrue(res is Error);
    Error err = <Error>res;
    test:assertEquals(err.message(), "Invalid statement ID format.");
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
