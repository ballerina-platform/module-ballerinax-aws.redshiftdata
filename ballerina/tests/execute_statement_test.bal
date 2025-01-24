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
import ballerina/test;

@test:Config {
    groups: ["execute"]
}
isolated function testBasicStatement() returns error? {
    Client redshift = check new Client(testConnectionConfig);
    ExecutionResponse res = check redshift->executeStatement(`SELECT * FROM Users`);

    test:assertTrue(res.statementId != "");
    test:assertTrue(res.createdAt[0] > 0);
    test:assertTrue(res.sessionId is ()); // Since we are not using sessionKeepAliveSeconds
    check redshift->close();
}

@test:Config {
    groups: ["execute"]
}
isolated function testSessionId() returns error? {
    ConnectionConfig connectionConfig = {
        region: testRegion,
        authConfig: testAuthConfig,
        dbAccessConfig: {
            id: testClusterId,
            database: testDatabaseName,
            dbUser: testDbUser,
            sessionKeepAliveSeconds: 3600
        }
    };
    Client redshift = check new Client(connectionConfig);
    ExecutionResponse res1 = check redshift->executeStatement(`SELECT * FROM Users`);
    test:assertTrue(res1.sessionId is string && res1.sessionId != "");

    runtime:sleep(2); // wait for session to establish
    ExecutionResponse res2 = check redshift->executeStatement(`SELECT * FROM Users`,
        {dbAccessConfig: res1.sessionId});
    test:assertTrue(res2.sessionId == res1.sessionId);
    check redshift->close();
}

@test:Config {
    groups: ["execute"]
}
isolated function testExecutionConfig() returns error? {
    Client redshift = check new Client(testConnectionConfig);
    ExecutionConfig config = {
        dbAccessConfig: testDbAccessConfig,
        clientToken: "testToken",
        statementName: "testStatement",
        withEvent: true
    };
    ExecutionResponse res = check redshift->executeStatement(`SELECT * FROM Users`, config);
    test:assertTrue(res.statementId != "");
    check redshift->close();
}

@test:Config {
    groups: ["execute"]
}
isolated function testParameterizedStatement() returns error? {
    Client redshift = check new Client(testConnectionConfig);
    string tableName = "Users";
    ExecutionResponse res = check redshift->executeStatement(`SELECT * FROM ${tableName}`);
    test:assertTrue(res.statementId != "");
    check redshift->close();
}

@test:Config {
    groups: ["execute"]
}
isolated function testNilParameterizedStatement() returns error? {
    Client redshift = check new Client(testConnectionConfig);
    string? username = ();
    ExecutionResponse res = check redshift->executeStatement(`SELECT * FROM User WHERE username = ${username}`);
    DescriptionResponse descRes = check redshift->describeStatement(res.statementId);
    test:assertEquals(descRes.queryString, "SELECT * FROM User WHERE username = NULL");
    check redshift->close();
}

@test:Config {
    groups: ["execute"]
}
isolated function testEmptyStatement() returns error? {
    Client redshift = check new Client(testConnectionConfig);
    ExecutionResponse|Error res = redshift->executeStatement(``);
    test:assertTrue(res is Error && (res.message() == "SQL statement cannot be empty."));
    check redshift->close();
}

@test:Config {
    groups: ["execute"]
}
isolated function testWithDbConfigs() returns error? {
    ConnectionConfig mockConnectionConfig = {
        region: testRegion,
        authConfig: testAuthConfig,
        dbAccessConfig: {
            id: "CLUSTER_ID",
            database: "",
            dbUser: ""
        }
    };
    Client redshift = check new Client(mockConnectionConfig);
    ExecutionResponse res = check redshift->executeStatement(`SELECT * FROM Users`,
        {dbAccessConfig: testDbAccessConfig});
    test:assertTrue(res.statementId != "");
    check redshift->close();
}

@test:Config {
    groups: ["execute"]
}
isolated function testWithInvalidDbConfigs() returns error? {
    ConnectionConfig mockConnectionConfig = {
        region: testRegion,
        authConfig: testAuthConfig,
        dbAccessConfig: {
            id: "clusterId",
            database: "dbName",
            dbUser: "dbUser"
        }
    };
    Client redshift = check new Client(mockConnectionConfig);
    ExecutionResponse|Error res = redshift->executeStatement(`SELECT * FROM Users`);
    test:assertTrue(res is Error);
    if res is Error {
        ErrorDetails errorDetails = res.detail();
        test:assertEquals(errorDetails.httpStatusCode, 400);
        test:assertEquals(errorDetails.errorMessage, "Redshift endpoint doesn't exist in this region.");
    }
    check redshift->close();
}

@test:Config {
    groups: ["execute"]
}
isolated function testWithInvalidStatementName() returns error? {
    Client redshift = check new Client(testConnectionConfig);
    ExecutionResponse|Error res = redshift->executeStatement(`SELECT * FROM Users`, statementName = "");
    test:assertTrue(res is Error);
    if res is Error {
        test:assertEquals(res.message(), "The statement name should be at least 1 character long.");
    }
    check redshift->close();
}

@test:Config {
    groups: ["execute"]
}
isolated function testWithInvalidClusterId() returns error? {
    Client redshift = check new Client(testConnectionConfig);
    ExecutionResponse|Error res = redshift->executeStatement(`SELECT * FROM Users`, dbAccessConfig = {
        id: "",
        database: testDatabaseName
    });
    if res is Error {
        test:assertEquals(res.message(), "The cluster ID should be at least 1 character long.");
    }
    check redshift->close();
}

@test:Config {
    groups: ["execute"]
}
isolated function testNoDbAccessConfig() returns error? {
    ConnectionConfig connectionConfig = {
        region: testRegion,
        authConfig: testAuthConfig,
        dbAccessConfig: ()
    };
    Client redshift = check new Client(connectionConfig);
    ExecutionResponse|Error res = redshift->executeStatement(`SELECT * FROM Users`);
    test:assertTrue(res is Error);
    if res is Error {
        test:assertEquals(res.message(), "Error occurred while executing the executeStatement: No database access " +
                "configuration provided in the initialization of the client or in the execute statement config");
    }
    check redshift->close();
}
