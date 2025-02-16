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

import ballerina/log;
import ballerina/test;

@test:BeforeSuite
function beforeFunction() returns error? {
    log:printInfo("Setting up tables");
    Client redshift = check new Client(testConnectionConfig);

    ExecutionResponse createSupportedTypes = check redshift->execute(`
        CREATE TABLE IF NOT EXISTS SupportedTypes (
        int_type INTEGER,
        bigint_type BIGINT,
        double_type DOUBLE PRECISION,
        boolean_type BOOLEAN,
        string_type VARCHAR(255),
        nil_type VARCHAR(255)
    );
    `);
    DescriptionResponse SupportedTypesDescription =
        check waitForCompletion(redshift, createSupportedTypes.statementId);
    test:assertEquals(SupportedTypesDescription.status, FINISHED);

    ExecutionResponse createUserTable = check redshift->execute(`
        CREATE TABLE Users (
        user_id INT,
        username VARCHAR(255),
        email VARCHAR(255),
        age INT
    );
    `);
    DescriptionResponse createUserTableDescription =
        check waitForCompletion(redshift, createUserTable.statementId);
    test:assertEquals(createUserTableDescription.status, FINISHED);

    ExecutionResponse insertUsers = check redshift->execute(`
        INSERT INTO Users (user_id, username, email, age) VALUES
        (1, 'JohnDoe', 'john.doe@example.com', 25),
        (2, 'JaneSmith', 'jane.smith@example.com', 30),
        (3, 'BobJohnson', 'bob.johnson@example.com', 22);
    `);
    DescriptionResponse insertUsersDescription =
        check waitForCompletion(redshift, insertUsers.statementId);
    test:assertEquals(insertUsersDescription.status, FINISHED);

    check redshift->close();
}

@test:AfterSuite
function afterFunction() returns error? {
    log:printInfo("Cleaning up resources");
    Client redshift = check new Client(testConnectionConfig);

    ExecutionResponse dropUsers = check redshift->execute(`DROP TABLE IF EXISTS Users`);
    DescriptionResponse dropUsersDescription = check waitForCompletion(redshift, dropUsers.statementId);
    test:assertEquals(dropUsersDescription.status, FINISHED);

    ExecutionResponse dropSupportedTypes = check redshift->execute(`DROP TABLE IF EXISTS SupportedTypes`);
    DescriptionResponse dropSupportedTypesDescription =
        check waitForCompletion(redshift, dropSupportedTypes.statementId);
    test:assertEquals(dropSupportedTypesDescription.status, FINISHED);

    check redshift->close();
}
