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

import ballerina/jballerina.java;
import ballerina/sql;

public isolated client class Client {

    # Initializes AWS Redshift Data API client.
    #
    # + connectionConfig - Configurations related to redshift data api
    # + return - The `redshiftdata:Client` or `redshiftdata:Error` if the initialization fails
    public isolated function init(*ConnectionConfig connectionConfig) returns Error? {
        return self.externInit(connectionConfig);
    }

    isolated function externInit(ConnectionConfig connectionConfig)
    returns Error? = @java:Method {
        name: "init",
        'class: "io.ballerina.lib.aws.redshiftdata.NativeClientAdaptor"
    } external;

    # Executes the SQL query.
    #
    # + sqlStatement - The SQL statement to be executed
    # + executeStatementConfig - The configurations related to the execution of the statement.
    # + return - The statementId that can be used to retrieve the results or an error
    remote isolated function executeStatement(sql:ParameterizedQuery sqlStatement,
            *ExecuteStatementConfig executeStatementConfig)
    returns ExecuteStatementResponse|Error {
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

    # Executes the SQL queries in a batch.
    #
    # + sqlStatements - The SQL statements to be executed
    # + executeStatementConfig - The configurations related to the execution of the statements.
    # + return - The statementIds that can be used to retrieve the results or an error
    remote isolated function batchExecuteStatement(SqlStatements sqlStatements,
            *ExecuteStatementConfig executeStatementConfig)
    returns BatchExecuteStatementResponse|Error = @java:Method {
        'class: "io.ballerina.lib.aws.redshiftdata.NativeClientAdaptor"
    } external;

    # Retrieves the results of a previously executed SQL statement.
    #
    # + statementId - The identifier of the SQL statement
    # + resultConfig - The configurations related to the execution of getting the results
    # + rowTypes - The typedesc of the record to which the result needs to be returned
    # + return - Stream of records in the type of rowTypes or an `redshiftdata:Error`
    remote isolated function getQueryResult(StatementId statementId,
            typedesc<record {}> rowTypes = <>, *ResultConfig resultConfig)
    returns stream<rowTypes, Error?>|Error = @java:Method {
        'class: "io.ballerina.lib.aws.redshiftdata.NativeClientAdaptor"
    } external;

    # Describes the details about a specific instance when a query was run by the Amazon Redshift Data API.
    #
    # + statementId - The identifier of the SQL statement
    # + resultConfig - The configurations related to the execution of getting the results
    # + return - The details about the execution of the statement or batch of statements or an `redshiftdata:Error`
    remote isolated function getExecutionResult(StatementId statementId, *ResultConfig resultConfig)
    returns ExecutionResult|Error = @java:Method {
        'class: "io.ballerina.lib.aws.redshiftdata.NativeClientAdaptor"
    } external;

    # Closes the AWS Redshift Data API client.
    # ```ballerina
    # check redshift->close();
    # ```
    #
    # + return - A `redshiftdata:Error` if there is an error while closing the client resources or else nil
    remote function close() returns Error? = @java:Method {
        'class: "io.ballerina.lib.aws.redshiftdata.NativeClientAdaptor"
    } external;
}

isolated function nextResult(ResultIterator iterator) returns record {}|Error? = @java:Method {
    'class: "io.ballerina.lib.aws.redshiftdata.QueryResultProcessor"
} external;

isolated function closeResult(ResultIterator iterator) returns Error? = @java:Method {
    'class: "io.ballerina.lib.aws.redshiftdata.QueryResultProcessor"
} external;
