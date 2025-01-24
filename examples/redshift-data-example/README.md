# Redshift Data Example

This example demonstrates how to use the Ballerina Redshift Data connector to perform SQL operations on an AWS Redshift cluster. It includes creating a table, inserting data, querying data, and dropping the table.

## Prerequisites

### 1. Set up

Ensure that you have the necessary AWS credentials and a Redshift cluster. Refer to the set up guide in [ReadMe](../../ballerina/Module.md) for additional details.

### 2. Configuration

Configure the AWS Redshift API credentials and database information in the `Config.toml` file located in the example directory:

```toml
accessKeyId="<Your AWS Access Key ID>"
secretAccessKey="<Your AWS Secret Access Key>"

[dbAccessConfig]
id="<Your Redshift Cluster ID>"
database="<Your Redshift Database Name>"
dbUser="<Your Redshift Database User>"
```

## Run the Example

Execute the following command to run the example:

```bash
bal run
```

## Code Walkthrough

1. **Redshift Client Initialization**: A `redshiftdata:Client` is created using the provided AWS credentials and Redshift cluster information.

1. **Creating a Table**: A `CREATE TABLE` SQL query is executed to create a Users table.

1. **Inserting Data**: An `INSERT` SQL query is executed to add sample data into the Users table.

1. **Querying Data**: A `SELECT` SQL query is used to retrieve all records from the Users table, and the results are printed to the console.

1. **Dropping the Table**: A `DROP TABLE` SQL query is executed to remove the Users table.

## Functions

- **waitForCompletion**: A utility function that polls the status of a Redshift statement execution until it is finished, failed, or aborted.
