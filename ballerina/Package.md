## Overview

[Amazon Redshift](https://docs.aws.amazon.com/redshift/latest/mgmt/welcome.html) is a powerful and fully-managed data warehouse service provided by Amazon Web Services (AWS), designed to efficiently analyze large datasets with high performance and scalability.

The `ballerinax/aws.redshiftdata` package allows developers to interact with Amazon Redshift Data API seamlessly using Ballerina. The Redshift Data API simplifies data access by eliminating the need for managing persistent database connections or the Redshift JDBC driver.

## Setup guide

### Login to AWS Console

Log into the [AWS Management Console](https://aws.amazon.com/).

### Create a user

1. In the AWS Management Console, search for IAM in the services search bar.
2. Click on IAM

![create-user-1.png](https://raw.githubusercontent.com/ballerina-platform/module-ballerinax-aws.redshiftdata/main/docs/setup/resources/create-user-1.png)

3. Click Users

![create-user-2.png](https://raw.githubusercontent.com/ballerina-platform/module-ballerinax-aws.redshiftdata/main/docs/setup/resources/create-user-2.png)

4. Click Create User

![create-user-3.png](https://raw.githubusercontent.com/ballerina-platform/module-ballerinax-aws.redshiftdata/main/docs/setup/resources/create-user-3.png)

5. Give user a name and click next

![specify-user-details.png](https://raw.githubusercontent.com/ballerina-platform/module-ballerinax-aws.redshiftdata/main/docs/setup/resources/specify-user-details.png)

6. Add necessary permissions by adding the user to a user group, copy permissions or directly attach the policies. And click next.

![set-user-permissions.png](https://raw.githubusercontent.com/ballerina-platform/module-ballerinax-aws.redshiftdata/main/docs/setup/resources/set-user-permissions.png)

7. Review and create the user

![review-create-user.png](https://raw.githubusercontent.com/ballerina-platform/module-ballerinax-aws.redshiftdata/main/docs/setup/resources/review-create-user.png)

### Get user access keys

1. Click the user that created

![users.png](https://raw.githubusercontent.com/ballerina-platform/module-ballerinax-aws.redshiftdata/main/docs/setup/resources/users.png)

2. Click `Create access key`

![create-access-key-1.png](https://raw.githubusercontent.com/ballerina-platform/module-ballerinax-aws.redshiftdata/main/docs/setup/resources/create-access-key-1.png)

3. Click your use case and click next.

![select-usecase.png](https://raw.githubusercontent.com/ballerina-platform/module-ballerinax-aws.redshiftdata/main/docs/setup/resources/select-usecase.png)

4. Record the Access Key and Secret access key. These credentials will be used to authenticate your Ballerina application with the Redshift cluster.

![retrieve-access-key.png](https://raw.githubusercontent.com/ballerina-platform/module-ballerinax-aws.redshiftdata/main/docs/setup/resources/retrieve-access-key.png)

### Setup a Cluster

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

### Note

Amazon Redshift now offers a serverless option, allowing you to use the data warehouse without managing infrastructure. Redshift Serverless automatically scales to handle your workloads, providing a flexible and efficient way to run analytics.
For configuring a **Redshift serverless** setup, please refer to this [link.](https://docs.aws.amazon.com/redshift/latest/gsg/new-user-serverless.html)

## Quickstart

To use the `aws.redshiftdata` connector in your Ballerina project, modify the .bal file as follows:

### Step 1: Import the module

```ballerina
import ballerinax/aws.redshiftdata;
```

### Step 2: Instantiate a new connector

Create a new `redshiftdata:Client` by providing the region, authConfig and dbAccessConfig.

The `dbAccessConfig` in the `ConnectionConfig` record defines the database access configuration for connecting to the Redshift Data API. It can be set to either a Cluster or a WorkGroup (Serverless mode). Additionally, users can override this configuration for specific requests by providing it in individual calls to methods like executeStatement or batchExecuteStatement, allowing for more granular control over database access per execution.

```ballerina
configurable string accessKeyId = ?;
configurable string secretAccessKey = ?;
configurable redshiftdata:Cluster dbAccessConfig = ?;

redshiftdata:Client redshiftdata = check new (region = redshiftdata:US_EAST_2,
        authConfig = {
            accessKeyId,
            secretAccessKey
        },
        dbAccessConfig
    );
```

### Step 3: Invoke the connector operations

Now, utilize the available connector operations.

```ballerina
redshiftdata:ExecutionResponse response = check redshiftdata->executeStatement(`SELECT * FROM Users`);

redshiftdata:DescriptionResponse descriptionResponse = check redshiftdata->describeStatement(response.statementId);

stream<User, redshiftdata:Error?> statementResult = check redshiftdata->getStatementResult(response.statementId);
```

### Step 4: Run the Ballerina application

Use the following command to compile and run the Ballerina program.

```bash
bal run
```

## Examples

The `aws.redshiftdata` connector provides practical examples illustrating usage in various scenarios. Explore these [examples](https://github.com/ballerina-platform/module-ballerinax-aws.redshiftdata/tree/main/examples).

1. [Manage Users](https://github.com/ballerina-platform/module-ballerinax-aws.redshiftdata/tree/main/examples/manage-users/) - This example demonstrates how to use the Ballerina Redshift Data connector to perform SQL operations on an AWS Redshift cluster. It includes creating a table, inserting data, querying data, and dropping the table.

2. [Music store](https://github.com/ballerina-platform/module-ballerinax-aws.redshiftdata/tree/main/examples/music-store) - This example illustrates the process of creating an HTTP RESTful API with Ballerina to perform basic CRUD operations on a database, specifically AWS Redshift, involving setup, configuration, and running examples.
