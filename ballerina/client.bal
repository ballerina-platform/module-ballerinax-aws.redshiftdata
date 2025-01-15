//  Copyright (c) 2024, WSO2 LLC. (http://www.wso2.org).
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

# The AWS Redshift Data client.
public isolated client class Client {

    # Initialize AWS Redshift Data client.
    # ```ballerina
    # redshiftdata:Client redshift = check new (region = "us-east-2",
    #    authConfig = {
    #        accessKeyId: "<aws-access-key>",
    #        secretAccessKey: "<aws-secret-key>"
    #    },
    #    dbAccessConfig = {
    #        id: CLUSTER_ID,
    #        database: DATABASE_NAME,
    #        dbUser: DB_USER
    #    }
    # );
    # ```
    #
    # + connectionConfig - The Redshift Data client configurations.
    # If you provide a `dbAccessConfig` this configuration will be used
    # when executing statements. You can also override this `dbAccessConfig` at the API level.
    # + return - The `redshiftdata:Client` or a `redshiftdata:Error` if the initialization fails.
    public isolated function init(*ConnectionConfig connectionConfig) returns Error? {
        ConnectionConfig|constraint:Error validated = constraint:validate(connectionConfig);
        if validated is constraint:Error {
            return error Error(string `Connection configuration validation failed: ${validated.message()}`);
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
    # redshiftdata:ExecuteStatementResponse response = check redshift->executeStatement(`SELECT * FROM Users`);
    # ```
    #
    # + sqlStatement - The SQL statement to be executed.
    # + executeStatementConfig - The configurations related to the execution of the statement.
    # + return - The `redshiftdata:ExecuteStatementResponse` or a `redshiftdata:Error` if the execution fails.
    remote isolated function executeStatement(sql:ParameterizedQuery sqlStatement,
            *ExecuteStatementConfig executeStatementConfig)
    returns ExecuteStatementResponse|Error {
        ExecuteStatementConfig|constraint:Error validated = constraint:validate(executeStatementConfig);
        if validated is constraint:Error {
            return error Error(string `Execute statement configuration validation failed: ${validated.message()}`);
        }
        if sqlStatement.strings.length() == 0 {
            return error Error("SQL statement cannot be empty.");
        }
        if sqlStatement.insertions.some(insertion => insertion is ()) {
            return error Error("SQL statement cannot have nil parameters.");
        }
        return self.externExecuteStatement(sqlStatement, executeStatementConfig);
    }

    isolated function externExecuteStatement(sql:ParameterizedQuery sqlStatement,
            ExecuteStatementConfig executeStatementConfig)
    returns ExecuteStatementResponse|Error = @java:Method {
        name: "executeStatement",
        'class: "io.ballerina.lib.aws.redshiftdata.NativeClientAdaptor"
    } external;

    # Runs one or more SQL statements, which can be data manipulation language (DML) or data definition language (DDL). The batch size should not exceed 40.
    # ```ballerina
    # redshiftdata:ExecuteStatementResponse response = check redshift->batchExecuteStatement([`<statement>`,
    #    `<statement>`]);
    # ```
    #
    # + sqlStatements - The SQL statements to be executed.
    # + executeStatementConfig - The configurations related to the execution of the statements.
    # + return - The `redshiftdata:ExecuteStatementResponse` or a `redshiftdata:Error` if the execution fails
    remote isolated function batchExecuteStatement(sql:ParameterizedQuery[] sqlStatements,
            *ExecuteStatementConfig executeStatementConfig)
    returns ExecuteStatementResponse|Error {
        ExecuteStatementConfig|constraint:Error validated = constraint:validate(executeStatementConfig);
        if validated is constraint:Error {
            return error Error(string `Execute statement configuration validation failed: ${validated.message()}`);
        }
        if sqlStatements.length() == 0 {
            return error Error("SQL statements cannot be empty.");
        }
        if sqlStatements.length() > 40 {
            return error Error("Number of SQL statements cannot exceed 40.");
        }
        if sqlStatements.some(sqlStatement => sqlStatement.strings.length() == 0) {
            return error Error("SQL statements cannot have empty strings.");
        }
        if sqlStatements.some(sqlStatement => sqlStatement.insertions.some(insertion => insertion is ())) {
            return error Error("SQL statements cannot have nil parameters.");
        }
        return self.externBatchExecuteStatement(sqlStatements, executeStatementConfig);
    }

    isolated function externBatchExecuteStatement(sql:ParameterizedQuery[] sqlStatements,
            *ExecuteStatementConfig executeStatementConfig)
    returns ExecuteStatementResponse|Error = @java:Method {
        name: "batchExecuteStatement",
        'class: "io.ballerina.lib.aws.redshiftdata.NativeClientAdaptor"
    } external;

    # Retrieves the results of a previously executed SQL statement.
    # ```ballerina
    # stream<User, Error?> response = check redshift->getStatementResult("<statement-id>");
    # ```
    #
    # + statementId - The identifier of the SQL statement.
    # + rowTypes - The typedesc of the record to which the result needs to be returned.
    # + return - Stream of records in the type of rowTypes or a `redshiftdata:Error` if the retrieval fails.
    remote isolated function getStatementResult(StatementId statementId, typedesc<record {}> rowTypes = <>)
    returns stream<rowTypes, Error?>|Error = @java:Method {
        'class: "io.ballerina.lib.aws.redshiftdata.NativeClientAdaptor"
    } external;

    # Describes the details about a specific instance when a query was run by the Amazon Redshift Data API.
    # ```ballerina
    # redshiftdata:DescribeStatementResponse response = check redshift->describeStatement("<statement-id>");
    # ```
    #
    # + statementId - The identifier of the SQL statement.
    # + return - The `redshiftdata:DescribeStatementResponse` or a `redshiftdata:Error` if the execution fails.
    remote isolated function describeStatement(StatementId statementId)
    returns DescribeStatementResponse|Error {
        StatementId|constraint:Error validated = constraint:validate(statementId);
        if validated is constraint:Error {
            return error Error(string `Statement ID validation failed: ${validated.message()}`);
        }
        return self.externDescribeStatement(statementId);
    };

    isolated function externDescribeStatement(StatementId statementId)
    returns DescribeStatementResponse|Error = @java:Method {
        name: "describeStatement",
        'class: "io.ballerina.lib.aws.redshiftdata.NativeClientAdaptor"
    } external;

    # Closes the AWS Redshift Data API client.
    # ```ballerina
    # check redshift->close();
    # ```
    #
    # + return - A `redshiftdata:Error` if there is an error while closing the client resources or else nil.
    remote function close() returns Error? = @java:Method {
        'class: "io.ballerina.lib.aws.redshiftdata.NativeClientAdaptor"
    } external;
}
