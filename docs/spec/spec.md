# Specification: Ballerina `aws.redshiftdata` Connector

_Authors_: @ayeshLK \
_Reviewers_: TBA \
_Created_: 2025/02/27 \
_Updated_: 2025/02/27 \
_Edition_: Swan Lake 

## Introduction

This is the specification for the `aws.redshiftdata` connector of [Ballerina language](https://ballerina.io/), which provides the 
functionality to execute database operations on an AWS Redshift cluster or an AWS Redshift Serverless work group using an HTTP API.

The `aws.redshiftdata` connector specification has evolved and may continue to evolve in the future. The released versions of the 
specification can be found under the relevant GitHub tag.

If you have any feedback or suggestions about the connector, start a discussion via a [GitHub issue](https://github.com/ballerina-platform/ballerina-standard-library/issues) 
or in the [Discord server](https://discord.gg/ballerinalang). Based on the outcome of the discussion, the specification and implementation can be updated. Community feedback 
is always welcome. Any accepted proposal which affects the specification is stored under `/stdlib/proposals` in the [Ballerina spec repository](https://github.com/ballerina-platform/ballerina-spec). 
Proposals under discussion can be found as a Github issue in the [Ballerina spec repository](https://github.com/ballerina-platform/ballerina-spec).

The conforming implementation of the specification is released to Ballerina Central. Any deviation from the specification is considered a bug.

## Contents

1. [Overview](#1-overview)
2. [Client](#1-client)
    * 2.1. [Configurations](#21-configurations)
    * 2.2. [Initialization](#22-initialization)
    * 2.3. [Execution configurations](#23-execution-configurations)
    * 2.4. [Functions](#24-functions)

## 1. Overview

Amazon Redshift is a fully managed, high-performance data warehouse service from AWS, designed for efficient analysis of large datasets with seamless scalability. It provides two primary APIs for executing database operations:  

1. **JDBC-Based API** – A traditional, stateful connection method that requires managing persistent database connections, making it suitable for applications requiring low-latency, high-throughput queries.  
2. **Redshift Data API** – A  stateless API that eliminates the need for managing persistent connections. It allows executing SQL commands over HTTPS and can be used with both **Redshift clusters** and **Redshift Serverless workgroups**.  

This specification outlines the implementation of a client API for connecting to an **AWS Redshift cluster** or an **AWS Redshift Serverless workgroup** using the **Redshift Data API**.

## 2. Client

The `redshiftdata:Client` object represents an AWS Redshift Data API client.

### 2.1. Configurations

### 2.2. Initialization

### 2.3. Execution configurations

### 2.4. Functions