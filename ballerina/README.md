## Overview

[Amazon Redshift](https://docs.aws.amazon.com/redshift/latest/mgmt/welcome.html) is a powerful and fully-managed data warehouse service provided by Amazon Web Services (AWS), designed to efficiently analyze large datasets with high performance and scalability.

The Amazon Redshift Data connector allows developers to interact with Amazon Redshift Data API seamlessly. The Redshift Data API simplifies data access by eliminating the need for managing persistent database connections or the Redshift JDBC driver.

### Key Features

- Simplifies data access by using the Redshift Data API
- Eliminates the need for managing persistent database connections
- Support for executing SQL statements and retrieving results as a stream
- Support for both Cluster and WorkGroup (Serverless mode) configurations
- GraalVM compatible for native image builds

## Setup guide

### Set up a Redshift cluster

To use the Ballerina AWS Redshift data connector, follow these steps to set up an Amazon Redshift cluster:

#### Step 1: Navigate to Amazon Redshift and create a cluster

1. In the AWS Management Console, search for Redshift in the services search bar.
2. Click on Amazon Redshift.

   ![create-cluster-1.png](https://raw.githubusercontent.com/ballerina-platform/module-ballerinax-aws.redshiftdata/main/docs/setup/resources/create-cluster-1.png)

3. Click on the `Create cluster` button to initiate the process of creating a new Amazon Redshift cluster.

   ![create-cluster-2.png](https://raw.githubusercontent.com/ballerina-platform/module-ballerinax-aws.redshiftdata/main/docs/setup/resources/create-cluster-2.png)

#### Step 2: Configure cluster settings

1. Configure your Redshift cluster settings, including cluster identifier, database name, credentials, and other relevant parameters.

   ![configure-cluster-1.png](https://raw.githubusercontent.com/ballerina-platform/module-ballerinax-aws.redshiftdata/main/docs/setup/resources/configure-cluster-1.png)

2. Configure security groups to control inbound and outbound traffic to your Redshift cluster. Ensure that your Ballerina application will have the necessary permissions to access the cluster.

   ![configure-security-groups.png](https://raw.githubusercontent.com/ballerina-platform/module-ballerinax-aws.redshiftdata/main/docs/setup/resources/configure-security-groups.png)

3. Record the username during the cluster configuration. This will be used to authenticate your Ballerina application with the Redshift cluster.

   ![database-configurations.png](https://raw.githubusercontent.com/ballerina-platform/module-ballerinax-aws.redshiftdata/main/docs/setup/resources/database-configurations.png)

4. Finally, review your configuration settings, and once satisfied, click `Create cluster` to launch your Amazon Redshift cluster.

#### Step 3: Wait for cluster availability

1. It may take some time for your Redshift cluster to be available. Monitor the cluster status in the AWS Console until it shows as "Available".

   ![wait-for-availability.png](https://raw.githubusercontent.com/ballerina-platform/module-ballerinax-aws.redshiftdata/main/docs/setup/resources/wait-for-availability.png)

### Obtain IAM user credentials

To create an IAM user and generate an access key, follow the [obtaining IAM user credentials](https://central.ballerina.io/ballerinax/aws/latest#obtaining-iam-user-credentials) guide.

Attach the Redshift Data API permissions your application needs to the user — the AWS managed `AmazonRedshiftDataFullAccess` policy grants full access, or scope a custom policy to only the `redshift-data` actions you call.

The `redshift-data` actions alone are not sufficient: the user also needs permission to obtain the database credential.

| Database Access Config | Additional action required |
|---|---|
| `Cluster` with `dbUser` | `redshift:GetClusterCredentials` |
| `WorkGroup` (serverless) | `redshift-serverless:GetCredentials` |
| either one, with `secretArn` | `secretsmanager:GetSecretValue` |

## Quickstart

To use the `aws.redshiftdata` connector in your Ballerina project, modify the .bal file as follows:

### Step 1: Import the module

```ballerina
import ballerinax/aws;
import ballerinax/aws.redshiftdata;
```

### Step 2: Instantiate a new connector

Create a new `redshiftdata:Client` by providing the region, authentication configurations and dbAccessConfig.

The `dbAccessConfig` in the `ConnectionConfig` record defines the database access configuration for connecting to the Redshift Data API. It can be set to either a Cluster or a WorkGroup (Serverless mode). Additionally, users can override this configuration for specific requests by providing it in individual calls to methods like execute or batchExecute, allowing for more granular control over database access per execution.

```ballerina
configurable string accessKeyId = ?;
configurable string secretAccessKey = ?;
configurable redshiftdata:Cluster dbAccessConfig = ?;

redshiftdata:Client redshiftdata = check new ({
   region: aws:US_EAST_2,
   auth: {
      accessKeyId,
      secretAccessKey
   },
   dbAccessConfig
});
```

#### Alternative authentication methods

##### Profile-based authentication

You can use AWS profile-based authentication as an alternative to static credentials.

```ballerina
redshiftdata:Client redshiftdata = check new ({
   region: aws:US_EAST_1,
   auth: {
      profileName: "myAwsProfile",
      credentialsFilePath: "/path/to/custom/credentials"
   }
});
```

##### Default credential provider chain

The standard default credential provider chain, trying each of the following in order and taking the first source that yields credentials:

1. Environment variables (`AWS_ACCESS_KEY_ID`/`AWS_SECRET_ACCESS_KEY`, and `AWS_WEB_IDENTITY_TOKEN_FILE` if set)
2. The shared config/credentials file's active profile (`AWS_PROFILE`, or `default` if unset) — which may itself resolve via SSO, an external process, or a chained `AssumeRole` call, depending on that profile's configuration
3. Container credentials (ECS/EKS)
4. EC2 instance profile (IMDS)

```ballerina
redshiftdata:Client redshiftdata = check new ({
   region: aws:US_EAST_1,
   auth: auth:DEFAULT_CREDENTIALS
});
```

> **Note:** Beyond the three options above, the `auth` field also accepts `auth:AssumeRoleConfig` (STS assume-role), `auth:WebIdentityConfig` (web identity / OIDC), `auth:SsoAuthConfig` (IAM Identity Center), and `auth:ProcessAuthConfig` (external credential process). See the [`Ballerina AWS`](https://central.ballerina.io/ballerinax/aws/latest) documentation for details.

### Step 3: Invoke the connector operations

Now, utilize the available connector operations.

```ballerina
redshiftdata:ExecutionResponse response = check redshiftdata->execute(`SELECT * FROM Users`);

redshiftdata:DescriptionResponse descriptionResponse = check redshiftdata->describe(response.statementId);

stream<User, redshiftdata:Error?> statementResult = check redshiftdata->getResultAsStream(response.statementId);
```

### Step 4: Run the Ballerina application

Use the following command to compile and run the Ballerina program.

```bash
bal run
```

## Examples

The `aws.redshiftdata` connector provides practical examples illustrating usage in various scenarios. Explore these [examples](https://github.com/ballerina-platform/module-ballerinax-aws.redshiftdata/tree/main/examples).

1. [Manage users](https://github.com/ballerina-platform/module-ballerinax-aws.redshiftdata/tree/main/examples/manage-users/) - This example demonstrates how to use the Ballerina Redshift Data connector to perform SQL operations on an AWS Redshift cluster. It includes creating a table, inserting data, and querying data.

2. [Music store](https://github.com/ballerina-platform/module-ballerinax-aws.redshiftdata/tree/main/examples/music-store) - This example illustrates the process of creating an HTTP RESTful API with Ballerina to perform basic CRUD operations on a database, specifically AWS Redshift, involving setup, configuration, and running examples.
