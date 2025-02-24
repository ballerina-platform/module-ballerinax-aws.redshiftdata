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
    groups: ["describe", "liveServer"]
}
isolated function testBasicDescribeStatement() returns error? {
    sql:ParameterizedQuery query = `SELECT * FROM Users;`;
    ExecutionResponse executionResponse = check redshiftData->execute(query);
    DescriptionResponse descriptionResponse = check waitForCompletion(redshiftData, executionResponse.statementId);

    test:assertEquals(descriptionResponse.status, FINISHED);
    test:assertTrue(descriptionResponse.statementId != "");
    test:assertTrue(descriptionResponse.createdAt[0] > 0);
    test:assertTrue(descriptionResponse.duration > 0d);
    test:assertTrue(descriptionResponse.redshiftPid > 0);
    test:assertTrue(descriptionResponse.sessionId is ());
    test:assertTrue(descriptionResponse.subStatements is ());
    test:assertEquals(descriptionResponse.hasResultSet, true);
    test:assertEquals(descriptionResponse.queryString, query.strings[0]);
    test:assertEquals(descriptionResponse.statementId, executionResponse.statementId);
}

@test:Config {
    groups: ["describe", "liveServer"]
}
isolated function testBatchDescribeStatement() returns error? {
    sql:ParameterizedQuery[] queries = [`SELECT * FROM Users`, `SELECT * FROM Users;`];
    ExecutionResponse res = check redshiftData->batchExecute(queries);
    DescriptionResponse descriptionResponse = check waitForCompletion(redshiftData, res.statementId);

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
}

@test:Config {
    groups: ["describe", "liveServer"]
}
isolated function testIncorrectStatementDescribeStatement() returns error? {
    ExecutionResponse executionResponse = check redshiftData->execute(`SELECT * FROM non_existent_table;`);
    DescriptionResponse descriptionResponse = check waitForCompletion(redshiftData, executionResponse.statementId);

    test:assertEquals(descriptionResponse.status, FAILED);
    test:assertTrue(descriptionResponse.'error is string);
    test:assertTrue(descriptionResponse.'error != "");
}

@test:Config {
    groups: ["describe", "liveServer"]
}
isolated function testIncorrectBatchStatementDescribeStatement() returns error? {
    sql:ParameterizedQuery[] queries = [`SELECT * FROM Users`, `SELECT * FROM non_existent_table;`];
    ExecutionResponse res = check redshiftData->batchExecute(queries);
    DescriptionResponse descriptionResponse = check waitForCompletion(redshiftData, res.statementId);

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
}

@test:Config {
    groups: ["describe", "liveServer"]
}
isolated function testDescribeStatementWithInvalidStatementId() returns error? {
    StatementId invalidStatementId = "InvalidStatementId";
    DescriptionResponse|Error res = redshiftData->describe(invalidStatementId);
    test:assertTrue(res is Error);
    if res is Error {
        test:assertEquals(res.message(), "Invalid statement ID format.");
    }
}

isolated function waitForCompletion(Client redshift, string statementId)
returns DescriptionResponse|Error {
    foreach int retryCount in 0 ... 9 {
        DescriptionResponse descriptionResponse = check redshift->describe(statementId);
        if descriptionResponse.status is FINISHED|FAILED|ABORTED {
            return descriptionResponse;
        }
        runtime:sleep(1);
    }
    return error("Statement execution did not finish within the expected time");
}
