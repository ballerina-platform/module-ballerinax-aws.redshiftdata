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
import ballerina/os;
import ballerina/test;

final string accessKeyId = os:getEnv("BALLERINA_AWS_TEST_ACCESS_KEY_ID");
final string secretAccessKey = os:getEnv("BALLERINA_AWS_TEST_SECRET_ACCESS_KEY");

final string clusterId = "ballerina-redshift-cluster";
final string database = "dev";
final string dbUser = "awsuser";

final readonly & Region awsRegion = US_EAST_1;

final readonly & Cluster dbAccessConfig = {
    id: clusterId,
    database,
    dbUser
};

final readonly & StaticAuthConfig authConfig = {
    accessKeyId,
    secretAccessKey
};

final Client redshiftData = check initClient();

isolated function initClient() returns Client|error {
    boolean enableTests = accessKeyId !is "" && secretAccessKey !is "";
    if enableTests {
        return new ({
            region: awsRegion,
            authConfig,
            dbAccessConfig
        });
    }
    return test:mock(Client);
}

@test:BeforeSuite
function beforeTestSuite() returns error? {
    log:printInfo("Setting up tables");

    ExecutionResponse createSupportedTypes = check redshiftData->execute(`
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
        check waitForCompletion(redshiftData, createSupportedTypes.statementId);
    test:assertEquals(SupportedTypesDescription.status, FINISHED);

    ExecutionResponse createUserTable = check redshiftData->execute(`
        CREATE TABLE Users (
        user_id INT,
        username VARCHAR(255),
        email VARCHAR(255),
        age INT
    );
    `);
    DescriptionResponse createUserTableDescription =
        check waitForCompletion(redshiftData, createUserTable.statementId);
    test:assertEquals(createUserTableDescription.status, FINISHED);

    ExecutionResponse insertUsers = check redshiftData->execute(`
        INSERT INTO Users (user_id, username, email, age) VALUES
        (1, 'JohnDoe', 'john.doe@example.com', 25),
        (2, 'JaneSmith', 'jane.smith@example.com', 30),
        (3, 'BobJohnson', 'bob.johnson@example.com', 22);
    `);
    DescriptionResponse insertUsersDescription =
        check waitForCompletion(redshiftData, insertUsers.statementId);
    test:assertEquals(insertUsersDescription.status, FINISHED);
}

@test:AfterSuite
function afterTestSuite() returns error? {
    log:printInfo("Cleaning up resources");

    ExecutionResponse dropUsers = check redshiftData->execute(`DROP TABLE IF EXISTS Users`);
    DescriptionResponse dropUsersDescription = check waitForCompletion(redshiftData, dropUsers.statementId);
    test:assertEquals(dropUsersDescription.status, FINISHED);

    ExecutionResponse dropSupportedTypes = check redshiftData->execute(`DROP TABLE IF EXISTS SupportedTypes`);
    DescriptionResponse dropSupportedTypesDescription =
        check waitForCompletion(redshiftData, dropSupportedTypes.statementId);
    test:assertEquals(dropSupportedTypesDescription.status, FINISHED);
    check redshiftData->close();
}
