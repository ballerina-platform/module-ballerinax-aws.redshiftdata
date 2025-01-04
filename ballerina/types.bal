// Copyright (c) 2024 WSO2 LLC. (http://www.wso2.com).
//
// WSO2 LLC. licenses this file to you under the Apache License,
// Version 2.0 (the "License"); you may not use this file except
// in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

import ballerina/constraint;
import ballerina/time;

# Additional configurations related to redshift data api
#
# + region - The AWS region with which the connector should communicate
# + authConfig - The authentication configurations for the redshift data api
# + databaseConfig - The database configurations
# This can be overridden in the individual execute and batchExecute requests.
public type ConnectionConfig record {|
    Region region;
    AuthConfig authConfig;
    DatabaseConfig databaseConfig?;
|};

# An Amazon Web Services region that hosts a set of Amazon services.
public enum Region {
    AF_SOUTH_1 = "af-south-1",
    AP_EAST_1 = "ap-east-1",
    AP_NORTHEAST_1 = "ap-northeast-1",
    AP_NORTHEAST_2 = "ap-northeast-2",
    AP_NORTHEAST_3 = "ap-northeast-3",
    AP_SOUTH_1 = "ap-south-1",
    AP_SOUTH_2 = "ap-south-2",
    AP_SOUTHEAST_1 = "ap-southeast-1",
    AP_SOUTHEAST_2 = "ap-southeast-2",
    AP_SOUTHEAST_3 = "ap-southeast-3",
    AP_SOUTHEAST_4 = "ap-southeast-4",
    AWS_CN_GLOBAL = "aws-cn-global",
    AWS_GLOBAL = "aws-global",
    AWS_ISO_GLOBAL = "aws-iso-global",
    AWS_ISO_B_GLOBAL = "aws-iso-b-global",
    AWS_US_GOV_GLOBAL = "aws-us-gov-global",
    CA_WEST_1 = "ca-west-1",
    CA_CENTRAL_1 = "ca-central-1",
    CN_NORTH_1 = "cn-north-1",
    CN_NORTHWEST_1 = "cn-northwest-1",
    EU_CENTRAL_1 = "eu-central-1",
    EU_CENTRAL_2 = "eu-central-2",
    EU_ISOE_WEST_1 = "eu-isoe-west-1",
    EU_NORTH_1 = "eu-north-1",
    EU_SOUTH_1 = "eu-south-1",
    EU_SOUTH_2 = "eu-south-2",
    EU_WEST_1 = "eu-west-1",
    EU_WEST_2 = "eu-west-2",
    EU_WEST_3 = "eu-west-3",
    IL_CENTRAL_1 = "il-central-1",
    ME_CENTRAL_1 = "me-central-1",
    ME_SOUTH_1 = "me-south-1",
    SA_EAST_1 = "sa-east-1",
    US_EAST_1 = "us-east-1",
    US_EAST_2 = "us-east-2",
    US_GOV_EAST_1 = "us-gov-east-1",
    US_GOV_WEST_1 = "us-gov-west-1",
    US_ISOB_EAST_1 = "us-isob-east-1",
    US_ISO_EAST_1 = "us-iso-east-1",
    US_ISO_WEST_1 = "us-iso-west-1",
    US_WEST_1 = "us-west-1",
    US_WEST_2 = "us-west-2"
}

# Auth configurations for the redshift data api
#
# + accessKeyId - The AWS access key ID.
# + secretAccessKey - The AWS secret access key.
# + sessionToken - The session token if the credentials are temporary.
public type AuthConfig record {|
    string accessKeyId;
    string secretAccessKey;
    string sessionToken?;
|};

# Database configurations
#
# User must provide either `databaseUser` or `secretArn`
#
# + clusterId - The cluster identifier.
# + databaseName - The name of the database.
# + databaseUser - The database user name.
# + secretArn - The name or ARN of the secret that enables access to the database.
public type DatabaseConfig record {|
    @constraint:String {
        minLength: 1,
        maxLength: 63
    }
    string clusterId?;
    string databaseName?;
    string databaseUser?;
    string secretArn?;
|};

# Configuration related to get the results
#
# + nextToken - A value that indicates the starting point for the next set of response records in a subsequent request.
# + timeout - The timeout to be used getting the query results and execution results in `seconds`
# + pollingInterval - The polling interval to be used getting the query results and execution results in `seconds`
public type ResultConfig record {|
    string nextToken?;
    decimal timeout = 30;
    decimal pollingInterval = 5;
|};

# Configuration related to execute statement.
#
# + databaseConfig - The database configurations.
# + clientToken - A unique, case-sensitive identifier that you provide to ensure the idempotency of the request.
# + statementName - The name of the SQL statement.
# + withEvent - A value that indicates whether to send an event to the Amazon EventBridge event bus after the SQL statement runs.
# + sessionId - The session identifier of the query.
# + sessionKeepAliveSeconds - The number of seconds to keep the session alive after the query finishes.
# + workgroupName - The serverless workgroup name or Amazon Resource Name (ARN).
public type ExecuteStatementConfig record {|
    DatabaseConfig databaseConfig?;
    string clientToken?;
    @constraint:String {
        minLength: 1,
        maxLength: 500
    }
    string statementName?;
    boolean withEvent?;
    @constraint:String {
        pattern: re `^[a-z0-9]{8}(-[a-z0-9]{4}){3}-[a-z0-9]{12}(:\d+)?$`
    }
    string sessionId?;
    @constraint:Int {
        minValue: 0,
        maxValue: 86400
    }
    int sessionKeepAliveSeconds?;
    @constraint:String {
        pattern: re `^(([a-z0-9-]+)|(arn:(aws(-[a-z]+)*):redshift-serverless:[a-z]{2}(-gov)?-[a-z]+-\d{1}:\d{12}:workgroup/[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}))$`
    }
    string workgroupName?;
|};

# The response from the `executeStatement` method.
#
# + createdAt - The date and time (UTC) the statement was created.
# + hasDbGroups - For responses, this returns true if the service returned a value for the DbGroups property.
# + dbGroups - A list of colon (:) separated names of database groups.
# + statementId - The identifier of the SQL statement whose results are to be fetched.
# + sessionId - The session identifier of the query.
# + workgroupName - The serverless workgroup name or Amazon Resource Name (ARN).
public type ExecuteStatementResponse record {|
    time:Utc createdAt;
    boolean hasDbGroups;
    string[] dbGroups;
    string statementId;
    string sessionId?;
    string workgroupName?;
|};

# One or more SQL statements to run.
@constraint:Array {
    minLength: 1,
    maxLength: 40
}
public type SqlStatements string[];

# The response from the `batchExecuteStatement` method.
#
# + subStatementIds - The statement IDs of the SQL statements. Which are used to retrieve the results.
public type BatchExecuteStatementResponse record {|
    *ExecuteStatementResponse;
    string[] subStatementIds;
|};

# Describes the details about a specific instance when a query was run by the Amazon Redshift Data API.
#
# + statementId - The identifier of the SQL statement whose results are to be fetched.
# + createdAt - The date and time (UTC) the statement was created. 
# + duration - The amount of time in seconds that the statement ran.
# + 'error - The error message from the cluster if the SQL statement encountered an error while running.
# + hasResultSet - A value that indicates whether the statement has a result set.
# + queryString - The SQL statement text.
# + redshiftQueryId - The identifier of the query generated by Amazon Redshift.
# + resultRows - Either the number of rows returned from the SQL statement or the number of rows affected.
# + resultSize - The size in bytes of the returned results.
# + status - The status of the SQL statement being described.
# + updatedAt - The date and time (UTC) that the metadata for the SQL statement was last updated.
# + hasQueryParameters - This returns true if the service returned a value for the QueryParameters property.
# + hasSubStatements - This returns true if the service returned a value for the SubStatements property.
# + redshiftPid - The process identifier from Amazon Redshift.
# + sessionId - The session identifier of the query.
# + subStatements - The SQL statements from a multiple statement run.
# + workgroupName - The serverless workgroup name or Amazon Resource Name (ARN).
public type ExecutionResult record {|
    string statementId;
    time:Utc createdAt;
    decimal duration;
    string 'error;
    boolean hasResultSet;
    string queryString;
    string redshiftQueryId;
    int resultRows;
    int resultSize;
    Status status;
    time:Utc updatedAt;
    boolean hasQueryParameters;
    boolean hasSubStatements;
    int redshiftPid;
    string sessionId;
    SubStatementData subStatements;
    string workgroupName;
|};

# Information about an SQL statement.
#
# + statementId - The identifier of the SQL statement.
# + createdAt - The date and time (UTC) the statement was created.  
# + duration - The amount of time in seconds that the statement ran. 
# + 'error - The error message from the cluster if the SQL statement encountered an error while running.
# + hasResultSet - A value that indicates whether the statement has a result set. 
# + queryString - The SQL statement text.
# + redshiftQueryId - The SQL statement identifier.
# + resultRows - Either the number of rows returned from the SQL statement or the number of rows affected.
# + resultSize - The size in bytes of the returned results.
# + status - The status of the SQL statement. 
# + updatedAt - The date and time (UTC) that the statement metadata was last updated.
public type SubStatementData record {|
    string statementId;
    time:Utc createdAt;
    decimal duration; // in seconds
    string 'error;
    boolean hasResultSet;
    string queryString;
    string redshiftQueryId;
    int resultRows;
    int resultSize;
    Status status;
    time:Utc updatedAt;
|};

public enum Status {
    SUBMITTED,
    PENDING,
    FAILED,
    SUCCESS
}

# The result iterator used to iterate results in stream returned from `getQueryResult` method.
#
# + isClosed - Indicates the stream state
class ResultIterator {
    private boolean isClosed = false;

    isolated function init() returns error? {
    }

    public isolated function next() returns record {|record {} value;|}|Error? {
        if self.isClosed {
            return error Error("Stream is closed. Therefore, no operations are allowed further on the stream.");
        }
        record {}|Error? result = nextResult(self);
        if result is record {} {
            record {|
                record {} value;
            |} streamRecord = {value: result};
            return streamRecord;
        } else if result is Error {
            self.isClosed = true;
            return result;
        } else {
            self.isClosed = true;
            return result;
        }
    }

    public isolated function close() returns Error? {
        if !self.isClosed {
            Error? e = closeResult(self);
            if e is () {
                self.isClosed = true;
            }
            return e;
        }
    }
}
