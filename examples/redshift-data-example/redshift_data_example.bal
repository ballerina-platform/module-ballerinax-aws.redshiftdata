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

import ballerina/io;
import ballerina/lang.runtime;
import ballerina/sql;
import ballerinax/aws.redshiftdata;

configurable string accessKeyId = ?;
configurable string secretAccessKey = ?;
configurable redshiftdata:Cluster dbAccessConfig = ?;

type User record {|
    int user_id;
    string username;
    string email;
    int age;
|};

public function main() returns error? {
    // Create a Redshift client
    redshiftdata:Client redshift = check new ({
        region: "us-east-2",
        authConfig: {
            accessKeyId: accessKeyId,
            secretAccessKey: secretAccessKey
        },
        dbAccessConfig: dbAccessConfig
    });

    // Create a table
    sql:ParameterizedQuery createTableQuery = `CREATE TABLE IF NOT EXISTS Users (
        user_id INT PRIMARY KEY,
        username VARCHAR(255),
        email VARCHAR(255),
        age INT
    );`;
    redshiftdata:ExecutionResponse createTableExecutionResponse = check redshift->executeStatement(createTableQuery);
    _ = check waitForDescribeStatementCompletion(redshift, createTableExecutionResponse.statementId);

    // Insert data into the table
    sql:ParameterizedQuery insertQuery = `INSERT INTO Users (user_id, username, email, age) VALUES
        (1, 'Alice', 'alice@gmail.com', 25),
        (2, 'Bob', 'bob@gmail.com', 30);`;
    redshiftdata:ExecutionResponse insertExecutionResponse = check redshift->executeStatement(insertQuery);
    redshiftdata:DescriptionResponse insertDescriptionResponse =
        check waitForDescribeStatementCompletion(redshift, insertExecutionResponse.statementId);
    io:println("Describe statement response for insert query: ", insertDescriptionResponse);

    // Select data from the table
    sql:ParameterizedQuery query = `SELECT * FROM Users;`;
    redshiftdata:ExecutionResponse res = check redshift->executeStatement(query);
    _ = check waitForDescribeStatementCompletion(redshift, res.statementId);
    stream<User, redshiftdata:Error?> resultStream = check redshift->getStatementResult(res.statementId);
    io:println("User details: ");
    check from User user in resultStream
        do {
            io:println(user);
        };

    // Drop the table
    sql:ParameterizedQuery dropTableQuery = `DROP TABLE Users;`;
    redshiftdata:ExecutionResponse dropTableExecutionResponse = check redshift->executeStatement(dropTableQuery);
    _ = check waitForDescribeStatementCompletion(redshift, dropTableExecutionResponse.statementId);
}

isolated function waitForDescribeStatementCompletion(redshiftdata:Client redshift, string statementId)
returns redshiftdata:DescriptionResponse|redshiftdata:Error {
    int i = 0;
    while (i < 10) {
        redshiftdata:DescriptionResponse|redshiftdata:Error describeStatementResponse =
            redshift->describeStatement(statementId);
        if (describeStatementResponse is redshiftdata:Error) {
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
