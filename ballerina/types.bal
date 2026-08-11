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
import ballerinax/aws;
import ballerinax/aws.auth;

# Represents connection configurations related to Redshift Data API.
#
# + auth - Authentication configuration: any standard credential source supported by
# AWS — static credentials, an AWS profile, STS assume-role,
# web identity (OIDC), IAM Identity Center (SSO), an external credential
# process, or the default credential provider chain
# + region - AWS region: an `aws:Region` enum member or a plain region
# string (e.g., `"us-east-1"`) for regions not yet in the enum
# + endpoint - Optional endpoint options: FIPS/dualstack variants, or a custom
# endpoint override (e.g. LocalStack, VPC interface endpoints)
# + dbAccessConfig - The database access configurations for the Redshift Data API
# This can be overridden in the individual `execute` and `batchExecute` requests
public type ConnectionConfig record {|
    auth:AuthConfig auth;
    aws:Region|string region;
    aws:EndpointConfig endpoint?;
    Cluster|WorkGroup dbAccessConfig?;
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

# Represents the configuration details required for connecting to an Amazon Redshift serverless workgroup.
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
# + withEvent - Flag which indicates to send an event after the SQL statement execution 
# to an event bus instance running in Amazon EventBridge
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
