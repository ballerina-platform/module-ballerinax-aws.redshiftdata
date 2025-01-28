# Developing a music store

This guide explains how to create an HTTP RESTful API using Ballerina for performing basic CRUD operations on a database by connecting to AWS Redshift Data API.

## Prerequisites

1. Ensure that you have the necessary AWS credentials and a Redshift cluster. Refer to the set up guide in [ReadMe](https://github.com/ballerina-platform/module-ballerinax-aws.redshiftdata/tree/main/README.md) for additional details.

2. Before running the service, you must first run the [`setup`](https://github.com/ballerina-platform/module-ballerinax-aws.redshiftdata/tree/main/examples/music-store/setup) Ballerina program. This step will create the necessary Album table and insert some dummy data required for the service to function.

### 1. Configuration

Configure the AWS Redshift API credentials and database information in `Config.toml` within the setup and music_store directories:

```toml
accessKeyId="<Your AWS Access Key ID>"
secretAccessKey="<Your AWS Secret Access Key>"

[dbAccessConfig]
id="<Your Redshift Cluster ID>"
database="<Your Redshift Database Name>"
dbUser="<Your Redshift Database User>"
```

### 2. Run the Example

1. First, run the [`setup`](https://github.com/ballerina-platform/module-ballerinax-aws.redshiftdata/tree/main/examples/music-store/setup) ballerina program to set up the `Album` table related to the sample. Execute the following command:

    ```bash
    cd setup
    bal run
    ```

1. Then to run the [`service`](https://github.com/ballerina-platform/module-ballerinax-aws.redshiftdata/tree/main/examples/music-store/service) example, execute the following command:

    ```bash
    cd service
    bal run
    ```
