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

public function main() returns error? {
    io:println("Setting up the Music Store database...");
    // Create a Redshift client
    redshiftdata:Client redshift = check new ({
        region: "us-east-2",
        authConfig: {
            accessKeyId: accessKeyId,
            secretAccessKey: secretAccessKey
        },
        dbAccessConfig: dbAccessConfig
    });

    // Creates `albums` table
    sql:ParameterizedQuery createTableQuery = `CREATE TABLE Albums (
        id VARCHAR(100) NOT NULL PRIMARY KEY,
        title VARCHAR(100),
        artist VARCHAR(100),
        price REAL
    );`;
    redshiftdata:ExecutionResponse createTableExecutionResponse = check redshift->executeStatement(createTableQuery);
    _ = check waitForDescribeStatementCompletion(redshift, createTableExecutionResponse.statementId);

    // Adds the records to the `albums` table
    sql:ParameterizedQuery[] insertQueries = [
        `INSERT INTO Albums VALUES('A-123', 'Lemonade', 'Beyonce', 18.98);`,
        `INSERT INTO Albums VALUES('A-321', 'Renaissance', 'Beyonce', 24.98);`
    ];
    redshiftdata:ExecutionResponse insertExecutionResponse =
        check redshift->batchExecuteStatement(insertQueries);
    _ = check waitForDescribeStatementCompletion(redshift, insertExecutionResponse.statementId);
    io:println("Music Store database setup completed successfully.");
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
