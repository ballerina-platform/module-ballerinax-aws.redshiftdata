# Specification: Ballerina `aws.redshiftdata` Connector

_Authors_: @ayeshLK \
_Reviewers_: @ThisaruGuruge @chathushkaayash \
_Created_: 2025/02/27 \
_Updated_: 2025/02/27 \
_Edition_: Swan Lake 

## Introduction

This is the specification for the `aws.redshiftdata` connector of [Ballerina language](https://ballerina.io/), which provides the functionality to execute database operations on an AWS Redshift cluster or an AWS Redshift Serverless work group using an HTTP API.

The `aws.redshiftdata` connector specification has evolved and may continue to evolve in the future. The released versions of the specification can be found under the relevant GitHub tag.

If you have any feedback or suggestions about the connector, start a discussion via a [GitHub issue](https://github.com/ballerina-platform/ballerina-standard-library/issues) or in the [Discord server](https://discord.gg/ballerinalang). Based on the outcome of the discussion, the specification and implementation can be updated. Community feedback is always welcome. Any accepted proposal which affects the specification is stored under `/stdlib/proposals` in the [Ballerina spec repository](https://github.com/ballerina-platform/ballerina-spec). Proposals under discussion can be found as a Github issue in the [Ballerina spec repository](https://github.com/ballerina-platform/ballerina-spec).

The conforming implementation of the specification is released to Ballerina Central. Any deviation from the specification is considered a bug.

## Contents

1. [Overview](#1-overview)
2. [Client](#1-client)
    * 2.1. [Configurations](#21-configurations)
    * 2.2. [Initialization](#22-initialization)
    * 2.3. [Execution configurations](#23-execution-configurations)
    * 2.4. [Functions](#24-functions)
3. [Example usage](#3-example-usage)

## 1. Overview

Amazon Redshift is a fully managed, high-performance data warehouse service from AWS, designed for efficient analysis of large datasets with seamless scalability. It provides two primary APIs for executing database operations:  

1. **JDBC-Based API** – A traditional, stateful connection method that requires managing persistent database connections, making it suitable for applications requiring low-latency, high-throughput queries.  
2. **Redshift Data API** – A  stateless API that eliminates the need for managing persistent connections. It allows executing SQL commands over HTTPS and can be used with both **Redshift clusters** and **Redshift Serverless workgroups**.  

This specification outlines the implementation of a client API for connecting to an **AWS Redshift cluster** or an **AWS Redshift Serverless workgroup** using the **Redshift Data API**.

## 2. Client

The `redshiftdata:Client` object represents an AWS Redshift Data API client.

### 2.1. Configurations

- `ConnectionConfig` record represents the connection configuration related to AWS Redshift Data API.

```ballerina
public type ConnectionConfig record {|
    # The AWS region with which the connector should communicate
    Region region;
    # The authentication configurations for the Redshift Data API
    StaticAuthConfig|EC2IAMRoleConfig authConfig;
    # The database access configurations for the Redshift Data API 
    # which can be overridden in the individual `execute` and `batchExecute` requests
    Cluster|WorkGroup dbAccessConfig?;
|};
```

- `Region` enum represents the AWS region that host the application which uses the connector.

```ballerina
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
```

- `StaticAuthConfig` record represents AWS static authentication configurations.  

```ballerina
public type StaticAuthConfig record {|
    # The AWS access key ID, used to identify the user interacting with AWS
    string accessKeyId;
    # The AWS secret access key, used to authenticate the user interacting with AWS
    string secretAccessKey;
    # The AWS session token, used for authenticating a user with temporary permission to a resource
    string sessionToken?;
|};
```

- `EC2IAMRoleConfig` record represents the EC2 IAM role based authentication configurations.

```ballerina
public type EC2IAMRoleConfig record {|
    # Configure the profile name used for loading IMDS-related configuration,
    # like the endpoint mode (IPv4 vs IPv6)
    string profileName?;
    # The path to the file containing the profile configuration
    string profileFile?;
|};
```

- `Cluster` record represents the configuration details required for connecting to an Amazon Redshift cluster.

```ballerina
public type Cluster record {|
    # The cluster identifier 
    string id;
    # The name of the database
    string database;
    # The database user name
    string dbUser?;
    # The name or ARN of the secret that enables access to the database
    string secretArn?;
    # The number of seconds to keep the session alive after the query finishes
    int sessionKeepAliveSeconds?;
|};
```

- `WorkGroup` record represents the configuration details required for connecting to an Amazon Redshift serverless workgroup

```ballerina
public type WorkGroup record {|
    # The serverless workgroup name or Amazon Resource Name (ARN)
    string name;
    # The name of the database
    string database;
    # The name or ARN of the secret that enables access to the database
    string secretArn?;
    # The number of seconds to keep the session alive after the query finishes
    int sessionKeepAliveSeconds?;
|};
```

### 2.2. Initialization

- A `redshiftdata:Client` can be initialized by providing the `redshiftdata:ConnectionConfig`.

```ballerina
# Initialize AWS Redshift Data API client.
# ```
# redshiftdata:Client redshiftdata = check new (region = redshiftdata:US_EAST_2,
#    authConfig = {
#        accessKeyId: "<aws-access-key>",
#        secretAccessKey: "<aws-secret-key>"
#    },
#    dbAccessConfig = {
#        id: "<cluster-id>",
#        database: "<database-name>",
#        dbUser: "<db-user>"
#    }
# );
#
# + connectionConfig - The Redshift Data API client configurations
# If a `dbAccessConfig` is provided, it will be used for the statement executions and it can be overridden 
# using the `dbAccessConfig` at the API level
# + return - The `redshiftdata:Client` or a `redshiftdata:Error` if the initialization fails
public isolated function init(*redshiftdata:ConnectionConfig connectionConfig) returns redshiftdata:Error?;
```

### 2.3. Execution configurations

- When executing a SQL statement on AWS redshift, the developer could provide additiona configurations which 
is related to the query execution. The `ExecutionConfig` record represents the additional configuration which 
could be used when executing a single SQL statement or a SQL statement batch.

```ballerina
public type ExecutionConfig record {|
    # The database access configurations for the Redshift Data
    Cluster|WorkGroup|SessionId dbAccessConfig?;
    # A unique, case-sensitive identifier that you provide to ensure the idempotency of the request
    string clientToken?;
    # The name of the SQL statement
    string statementName?;
    # Flag which indicates to send an event after the SQL statement execution to 
    # an event bus instance running in Amazon EventBridge
    boolean withEvent?;
|};
```

### 2.4. Functions

- To run a SQL statement on AWS Redshift instance, `execute` function can be used.

```ballerina
# Runs an SQL statement, which can be data manipulation language (DML) or data definition language (DDL).
# ```
# redshiftdata:ExecutionResponse response = check redshiftdata->execute(`SELECT * FROM Users`);
# ```
#
# + statement - The SQL statement to be executed
# + executionConfig - The configurations related to the execution of the statement
# + return - The `redshiftdata:ExecutionResponse` or a `redshiftdata:Error` if the execution fails
remote isolated function execute(sql:ParameterizedQuery statement, *redshift:ExecutionConfig executionConfig) returns redshift:ExecutionResponse|redshift:Error;
```

- To run multiple SQL statements on AWS Redshift instance, `batchExecute` function can be used.

```ballerina
# Runs one or more SQL statements, which can be data manipulation language (DML) or data definition language (DDL). 
# The batch size should not exceed 40.
# ```
# redshiftdata:ExecutionResponse response = check redshiftdata->batchExecute([`<statement>`, `<statement>`]);
# ```
#
# + statements - The SQL statements to be executed
# + executionConfig - The configurations related to the execution of the statements
# + return - The `redshiftdata:ExecutionResponse` or a `redshiftdata:Error` if the execution fails
remote isolated function batchExecute(sql:ParameterizedQuery[] statements, *redshift:ExecutionConfig executionConfig) 
returns redshift:ExecutionResponse|redshift:Error;
```

- To retrieve the results for a previously executed SQL statement, `getResultAsStream` function can be used.

```ballerina
# Retrieves the results for a previously executed SQL statement.
# ```
# stream<User, Error?> response = check redshiftdata->getResultAsStream("<statement-id>");
# ```
#
# + statementId - The identifier of the SQL statement
# + rowTypes - The typedesc of the record to which the result needs to be returned
# + return - Stream of records in the type of rowTypes or a `redshiftdata:Error` if the retrieval fails
remote isolated function getResultAsStream(redshift:StatementId statementId, typedesc<record {}> rowTypes = <>) returns stream<rowTypes, redshift:Error?>|redshift:Error;
```

- To retrieve the execution status for a previously executed SQL statement, `describe` function can be used.

```ballerina
# Retrieves the execution status for a previously executed SQL statement.
# ```
# redshiftdata:DescriptionResponse response = check redshiftdata->describe("<statement-id>");
# ```
#
# + statementId - The identifier of the SQL statement
# + return - The `redshiftdata:DescriptionResponse` or a `redshiftdata:Error` if the execution fails
remote isolated function describe(redshiftdata:StatementId statementId) returns redshiftdata:DescriptionResponse|redshiftdata:Error;
```

- To gracefully close the AWS Redshift Data API client resources, `close` function can be used.

```ballerina
# Gracefully close and AWS Redshift Data API client resources.
# ```
# check redshiftdata->close();
# ```
#
# + return - A `redshiftdata:Error` if there is an error while closing the client resources or else nil
remote isolated function close() returns redshiftdata:Error?;
```

## 3. Example usage

### 3.1. Executing a single SQL statement

```ballerina
// Execute the SQL statement
redshiftdata:ExecutionResponse statementResponse = check redshiftData->execute(`SELECT * FROM Users`);
string statementId = statementResponse.statementId;

// Check the execution state by calling the `describe` function
redshiftdata:DescriptionResponse statementResult = check redshiftData->describe(statementId);

// If the statement execution is completed, retrieve the results
if statementResult.status is redshiftdata:FINISHED {
    stream<User, redshiftdata:Error?> userStream = check redshiftData->getResultAsStream(statementId);
}
```

### 3.2. Executing a batch of SQL statements

```ballerina
User[] users = [
    {userId: 1, username: "Alice", email: "alice@gmail.com", age: 25},
    {userId: 2, username: "Bob", email: "bob@gmail.com", age: 30}
];

sql:ParameterizedQuery[] insertStatements = from var row in users
        select `INSERT INTO Users (user_id, username, email, age) VALUES
            (${row.userId}, ${row.username}, ${row.email}, ${row.age})`;

// Execute the SQL statement batch
redshiftdata:ExecutionResponse statementBatchResponse = check redshiftData->batchExecute(insertStatements);
string statementId = statementBatchResponse.statementId;

// Check the execution state by calling the `describe` function
redshiftdata:DescriptionResponse statementBatchResult = check redshiftData->describe(statementId);

if statementBatchResult.status is redshiftdata:FINISHED {
    // implement custom logic here
}
```
