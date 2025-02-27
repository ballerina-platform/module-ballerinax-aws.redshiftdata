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
    @sql:Column {name: "user_id"}
    int userId;
    string username;
    string email;
    int age;
|};

public function main() returns error? {
    // Create a Redshift client
    redshiftdata:Client redshift = check new ({
        region: redshiftdata:US_EAST_2,
        auth: {
            accessKeyId,
            secretAccessKey
        },
        dbAccessConfig
    });

    // Create a table
    sql:ParameterizedQuery createTableQuery = `CREATE TABLE IF NOT EXISTS Users (
        user_id INT PRIMARY KEY,
        username VARCHAR(255),
        email VARCHAR(255),
        age INT
    );`;
    redshiftdata:ExecutionResponse createTableResponse = check redshift->execute(createTableQuery);
    _ = check waitForCompletion(redshift, createTableResponse.statementId);

    // Insert data into the table
    User[] users = [
        {userId: 1, username: "Alice", email: "alice@gmail.com", age: 25},
        {userId: 2, username: "Bob", email: "bob@gmail.com", age: 30}
    ];
    sql:ParameterizedQuery[] insertQueries = from var row in users
        select `INSERT INTO Users (user_id, username, email, age) VALUES
            (${row.userId}, ${row.username}, ${row.email}, ${row.age});`;

    redshiftdata:ExecutionResponse insertResponse = check redshift->batchExecute(insertQueries);
    redshiftdata:DescriptionResponse insertDescription = check waitForCompletion(redshift, insertResponse.statementId);
    io:println("Describe statement response for insert query: ", insertDescription);

    // Select data from the table
    redshiftdata:ExecutionResponse res = check redshift->execute(`SELECT * FROM Users;`);
    _ = check waitForCompletion(redshift, res.statementId);
    stream<User, redshiftdata:Error?> resultStream = check redshift->getResultAsStream(res.statementId);
    io:println("User details: ");
    check from User user in resultStream
        do {
            io:println(user);
        };
}

isolated function waitForCompletion(redshiftdata:Client redshift, string statementId)
returns redshiftdata:DescriptionResponse|redshiftdata:Error {
    foreach int retryCount in 0 ... 9 {
        redshiftdata:DescriptionResponse descriptionResponse = check redshift->describe(statementId);
        if descriptionResponse.status is redshiftdata:FINISHED {
            return descriptionResponse;
        }
        if descriptionResponse.status is redshiftdata:FAILED|redshiftdata:ABORTED {
            return error("Execution did not finish successfully. Status: " + descriptionResponse.status);
        }
        runtime:sleep(1);
    }
    return error("Statement execution did not finish within the expected time");
}
