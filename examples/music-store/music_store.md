# Working with AWS Redshiftdata

This guide explains how to create an HTTP RESTful API using Ballerina for performing basic CRUD operations on a database by connecting to AWS Redshift Data.

### 1. Set up

Ensure that you have the necessary AWS credentials and a Redshift cluster. Refer to the set up guide in [ReadMe](../../ballerina/Module.md) for additional details.

### 2. Configuration

Configure the AWS Redshift API credentials and database information in `Config.toml` within the setup and music_store directories:

```toml
accessKeyId="<Your AWS Access Key ID>"
secretAccessKey="<Your AWS Secret Access Key>"
databaseName="<Your Redshift Database Name>"
clusterId="<Your Redshift Cluster ID>"
dbUser="<Your Redshift Database User>"
```

## Run the Example

1. First, run the [`setup`](./setup/) ballerina program to set up the `Album` table related to the sample. Execute the following command:

```bash
cd setup
bal run
```

2. Then to run the `music_store` example, execute the following command:

```bash
cd music_store
bal run
```
