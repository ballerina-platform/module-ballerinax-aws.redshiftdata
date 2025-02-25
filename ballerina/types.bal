// Copyright (c) 2025, WSO2 LLC. (http://www.wso2.com).
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

# Additional configurations related to Redshift Data API
#
# + region - The AWS region with which the connector should communicate
# + auth - The authentication configurations for the Redshift Data API
# + dbAccessConfig - The database access configurations for the Redshift Data API
# This can be overridden in the individual `execute` and `batchExecute` requests
public type ConnectionConfig record {|
    Region region;
    StaticAuthConfig|EC2_IAM_ROLE auth;
    Cluster|WorkGroup dbAccessConfig?;
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

# Auth configurations for the Redshift Data API.
#
# + accessKeyId - The AWS access key ID, used to identify the user interacting with AWS
# + secretAccessKey - The AWS secret access key, used to authenticate the user interacting with AWS
# + sessionToken - The AWS session token, used for authenticating a user with temporary permission to a resource
public type StaticAuthConfig record {|
    string accessKeyId;
    string secretAccessKey;
    string sessionToken?;
|};

# Represents the EC2 IAM role based authentication for the Redshift Data API.
#
# + profileName - Configure the profile name used for loading IMDS-related configuration,
# like the endpoint mode (IPv4 vs IPv6)
# + profileFile - The path to the file containing the profile configuration
public type EC2_IAM_ROLE record {|
    string profileName?;
    string profileFile?;
|};

# Represents the configuration details required for connecting to an Amazon Redshift cluster.
#
# + id - The cluster identifier 
# + database - The name of the database
# + dbUser - The database user name 
# + secretArn - The name or ARN of the secret that enables access to the database
# + sessionKeepAliveSeconds - The number of seconds to keep the session alive after the query finishes
public type Cluster record {|
    @constraint:String {
        minLength: {
            value: 1,
            message: "The cluster ID should be at least 1 character long"
        },
        maxLength: {
            value: 63,
            message: "The cluster ID should be at most 63 characters long"
        }
    }
    string id;
    string database;
    string dbUser?;
    string secretArn?;
    @constraint:Int {
        minValue: {
            value: 0,
            message: "The sessionKeepAliveSeconds should be greater than or equal to 0"
        },
        maxValue: {
            value: 86400,
            message: "The sessionKeepAliveSeconds should be less than or equal to 86400"
        }
    }
    int sessionKeepAliveSeconds?;
|};

# Represents the configuration details required for connecting to an Amazon Redshift workgroup.
#
# + name - The serverless workgroup name or Amazon Resource Name (ARN)
# + database - The name of the database 
# + secretArn - The name or ARN of the secret that enables access to the database
# + sessionKeepAliveSeconds - The number of seconds to keep the session alive after the query finishes
public type WorkGroup record {|
    string name;
    string database;
    string secretArn?;
    @constraint:Int {
        minValue: {
            value: 0,
            message: "The sessionKeepAliveSeconds should be greater than or equal to 0"
        },
        maxValue: {
            value: 86400,
            message: "The sessionKeepAliveSeconds should be less than or equal to 86400"
        }
    }
    int sessionKeepAliveSeconds?;
|};

# The session identifier of the query.
@constraint:String {
    pattern: {
        value: re `^[a-z0-9]{8}(-[a-z0-9]{4}){3}-[a-z0-9]{12}(:\d+)?$`,
        message: "Invalid session ID format"
    }
}
public type SessionId string;

# Represents the configuration details required for `execute` method.
#
# + dbAccessConfig - The database access configurations for the Redshift Data
# If a `dbAccessConfig` is provided in the ExecutionConfig , it will override the init level dbAccessConfig
# + clientToken - A unique, case-sensitive identifier that you provide to ensure the idempotency of the request 
# + statementName - The name of the SQL statement
# + withEvent - A value that indicates whether to send an event to the Amazon EventBridge event bus after the SQL 
# statement runs
public type ExecutionConfig record {|
    Cluster|WorkGroup|SessionId dbAccessConfig?;
    string clientToken?;
    @constraint:String {
        minLength: {
            value: 1,
            message: "The statement name should be at least 1 character long"
        },
        maxLength: {
            value: 500,
            message: "The statement name should be at most 500 characters long"
        }
    }
    string statementName?;
    boolean withEvent?;
|};

# The response from the `execute` method.
#
# + createdAt - The date and time (UTC) the statement was created
# + dbGroups - A list of colon (:) separated names of database groups
# + statementId - The identifier of the SQL statement whose results are to be fetched
# + sessionId - The session identifier of the query
public type ExecutionResponse record {|
    time:Utc createdAt;
    string[] dbGroups?;
    StatementId statementId;
    SessionId sessionId?;
|};

# The identifier of the SQL statement
@constraint:String {
    pattern: {
        message: "Invalid statement ID format",
        value: re `^[a-z0-9]{8}(-[a-z0-9]{4}){3}-[a-z0-9]{12}(:\d+)?$`
    }
}
public type StatementId string;

# Describes the details about a specific instance when a query was run by the Amazon Redshift Data API.
#
# + subStatements - The SQL statements from a multiple statement run
# + redshiftPid - The process identifier from Amazon Redshift
# + sessionId - The session identifier of the query
public type DescriptionResponse record {|
    *StatementData;
    StatementData[] subStatements?;
    int redshiftPid;
    SessionId sessionId?;
|};

# Information about an SQL statement.
#
# + statementId - The identifier of the SQL statement described
# + createdAt - The date and time (UTC) when the SQL statement was submitted to run 
# + duration - The amount of time in seconds that the statement ran 
# + 'error - The error message from the cluster if the SQL statement encountered an error while running
# + hasResultSet - A value that indicates whether the statement has a result set 
# + queryString - The SQL statement text
# + redshiftQueryId - The identifier of the query generated by Amazon Redshift
# + resultRows - Either the number of rows returned from the SQL statement or the number of rows affected
# + resultSize - The size in bytes of the returned results
# + status - The status of the SQL statement being described
# + updatedAt - The date and time (UTC) that the statement metadata was last updated
public type StatementData record {|
    StatementId statementId;
    time:Utc createdAt;
    decimal duration;
    string 'error?;
    boolean hasResultSet;
    string queryString?;
    int redshiftQueryId;
    int resultRows;
    int resultSize;
    Status status;
    time:Utc updatedAt;
|};

# The status of the SQL statement being described. 
#
# + SUBMITTED - The query was submitted, but not yet processed
# + PICKED - The query has been chosen to be run
# + STARTED - The query run has started
# + FINISHED - The query has finished running
# + ABORTED - The query run was stopped by the user
# + FAILED - The query run failed
# + ALL - A status value that includes all query statuses. This value can be used to filter results
public enum Status {
    SUBMITTED,
    PICKED,
    STARTED,
    FINISHED,
    ABORTED,
    FAILED,
    ALL
}
