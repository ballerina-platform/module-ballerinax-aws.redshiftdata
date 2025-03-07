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
//  

import ballerina/constraint;
import ballerina/jballerina.java;
import ballerina/sql;

# The AWS Redshift Data API client.
public isolated client class Client {

    # Initialize AWS Redshift Data API client.
    # ```ballerina
    # redshiftdata:Client redshift = check new (region = redshiftdata:US_EAST_2,
    #    auth = {
    #        accessKeyId: "<aws-access-key>",
    #        secretAccessKey: "<aws-secret-key>"
    #    },
    #    dbAccessConfig = {
    #        id: "<cluster-id>",
    #        database: "<database-name>",
    #        dbUser: "<db-user>"
    #    }
    # );
    # ```
    #
    # + connectionConfig - The Redshift Data API client configurations
    # If a `dbAccessConfig` is provided, it will be used for the statement executions
    # It can be overridden using the `dbAccessConfig` at the API level
    # + return - The `redshiftdata:Client` or a `redshiftdata:Error` if the initialization fails
    public isolated function init(*ConnectionConfig connectionConfig) returns Error? {
        if connectionConfig.dbAccessConfig !is () {
            Cluster|WorkGroup|constraint:Error validationResult = constraint:validate(connectionConfig.dbAccessConfig);
            if validationResult is constraint:Error {
                return error Error(validationResult.message(), validationResult.cause());
            }
        }
        return self.externInit(connectionConfig);
    }

    isolated function externInit(ConnectionConfig connectionConfig)
    returns Error? = @java:Method {
        name: "init",
        'class: "io.ballerina.lib.aws.redshiftdata.NativeClientAdaptor"
    } external;

    # Runs an SQL statement, which can be data manipulation language (DML) or data definition language (DDL).
    # ```ballerina
    # redshiftdata:ExecutionResponse response = check redshift->execute(`SELECT * FROM Users`);
    # ```
    #
    # + statement - The SQL statement to be executed
    # + executionConfig - The configurations related to the execution of the statement
    # + return - The `redshiftdata:ExecutionResponse` or a `redshiftdata:Error` if the execution fails
    remote isolated function execute(sql:ParameterizedQuery statement, *ExecutionConfig executionConfig)
    returns ExecutionResponse|Error {
        _ = check self.validateExecutionConfig(executionConfig);
        if statement.strings.length() == 0 {
            return error Error("SQL statement cannot be empty.");
        }
        return self.externExecute(statement, executionConfig);
    }

    isolated function externExecute(sql:ParameterizedQuery statement,
            ExecutionConfig executionConfig)
    returns ExecutionResponse|Error = @java:Method {
        name: "execute",
        'class: "io.ballerina.lib.aws.redshiftdata.NativeClientAdaptor"
    } external;

    # Runs one or more SQL statements, which can be data manipulation language (DML) or data definition language (DDL).
    # The batch size should not exceed 40.
    # ```ballerina
    # redshiftdata:ExecutionResponse response = check redshift->batchExecute([`<statement>`,
    #    `<statement>`]);
    # ```
    #
    # + statements - The SQL statements to be executed
    # + executionConfig - The configurations related to the execution of the statements
    # + return - The `redshiftdata:ExecutionResponse` or a `redshiftdata:Error` if the execution fails
    remote isolated function batchExecute(sql:ParameterizedQuery[] statements,
            *ExecutionConfig executionConfig)
    returns ExecutionResponse|Error {
        _ = check self.validateExecutionConfig(executionConfig);
        if statements.length() == 0 {
            return error Error("SQL statements cannot be empty.");
        }
        if statements.length() > 40 {
            return error Error("Number of SQL statements cannot exceed 40.");
        }
        if statements.some(statement => statement.strings.length() == 0) {
            return error Error("SQL statements cannot have empty strings.");
        }
        return self.externBatchExecute(statements, executionConfig);
    }

    isolated function externBatchExecute(sql:ParameterizedQuery[] statements,
            *ExecutionConfig executionConfig)
    returns ExecutionResponse|Error = @java:Method {
        name: "batchExecute",
        'class: "io.ballerina.lib.aws.redshiftdata.NativeClientAdaptor"
    } external;

    # Retrieves the results for a previously executed SQL statement.
    # ```ballerina
    # stream<User, Error?> response = check redshift->getResultAsStream("<statement-id>");
    # ```
    #
    # + statementId - The identifier of the SQL statement
    # + rowTypes - The typedesc of the record to which the result needs to be returned
    # + return - Stream of records in the type of rowTypes or a `redshiftdata:Error` if the retrieval fails
    remote isolated function getResultAsStream(StatementId statementId, typedesc<record {}> rowTypes = <>)
    returns stream<rowTypes, Error?>|Error = @java:Method {
        'class: "io.ballerina.lib.aws.redshiftdata.NativeClientAdaptor"
    } external;

    # Retrieves the execution status for a previously executed SQL statement.
    # ```ballerina
    # redshiftdata:DescriptionResponse response = check redshift->describe("<statement-id>");
    # ```
    #
    # + statementId - The identifier of the SQL statement
    # + return - The `redshiftdata:DescriptionResponse` or a `redshiftdata:Error` if the execution fails
    remote isolated function describe(StatementId statementId)
    returns DescriptionResponse|Error {
        StatementId|constraint:Error validationResult = constraint:validate(statementId);
        if validationResult is constraint:Error {
            return error Error(validationResult.message(), validationResult.cause());
        }
        return self.externDescribe(statementId);
    };

    isolated function externDescribe(StatementId statementId)
    returns DescriptionResponse|Error = @java:Method {
        name: "describe",
        'class: "io.ballerina.lib.aws.redshiftdata.NativeClientAdaptor"
    } external;

    # Gracefully closes AWS Redshift Data API client resources.
    # ```ballerina
    # check redshift->close();
    # ```
    #
    # + return - A `redshiftdata:Error` if there is an error while closing the client resources or else nil
    remote isolated function close() returns Error? = @java:Method {
        'class: "io.ballerina.lib.aws.redshiftdata.NativeClientAdaptor"
    } external;

    private isolated function validateExecutionConfig(ExecutionConfig executionConfig)
    returns Error? {
        ExecutionConfig|constraint:Error configValidationResult = constraint:validate(executionConfig);
        if configValidationResult is constraint:Error {
            return error Error(configValidationResult.message(), configValidationResult.cause());
        }
        if executionConfig.dbAccessConfig !is () {
            Cluster|WorkGroup|SessionId|constraint:Error dbValidationResult =
                constraint:validate(executionConfig.dbAccessConfig);
            if dbValidationResult is constraint:Error {
                return error Error(dbValidationResult.message(), dbValidationResult.cause());
            }
        }
    }
}
