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
    redshiftdata:Client redshiftdata = check new ({
        region: redshiftdata:US_EAST_2,
        authConfig: {
            accessKeyId,
            secretAccessKey
        },
        dbAccessConfig
    });

    // Creates `albums` table
    sql:ParameterizedQuery createTableQuery = `CREATE TABLE Albums (
        id VARCHAR(100) NOT NULL PRIMARY KEY,
        title VARCHAR(100),
        artist VARCHAR(100),
        price REAL
    );`;
    redshiftdata:ExecutionResponse createTableExecutionResponse = check redshiftdata->executeStatement(createTableQuery);
    _ = check waitForCompletion(redshiftdata, createTableExecutionResponse.statementId);

    // Adds the records to the `albums` table
    sql:ParameterizedQuery[] insertQueries = [
        `INSERT INTO Albums VALUES('A-123', 'Lemonade', 'Beyonce', 18.98);`,
        `INSERT INTO Albums VALUES('A-321', 'Renaissance', 'Beyonce', 24.98);`
    ];
    redshiftdata:ExecutionResponse insertExecutionResponse =
        check redshiftdata->batchExecuteStatement(insertQueries);
    _ = check waitForCompletion(redshiftdata, insertExecutionResponse.statementId);
    io:println("Music Store database setup completed successfully.");
}

isolated function waitForCompletion(redshiftdata:Client redshiftdata, string statementId)
returns redshiftdata:DescriptionResponse|redshiftdata:Error {
    int i = 0;
    while i < 10 {
        redshiftdata:DescriptionResponse|redshiftdata:Error describeStatementResponse =
            redshiftdata->describeStatement(statementId);
        if describeStatementResponse is redshiftdata:Error {
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
