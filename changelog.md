# Change Log
This file contains all the notable changes done to the Ballerina AWS Redshift Data package through the releases.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/), and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## Unreleased

This release revamps the connector's authentication and region configuration to use the shared
[`ballerinax/aws`](https://github.com/ballerina-platform/module-ballerinax-aws) package, so that all AWS
connectors share a single, consistent credential model.

It contains breaking changes. See the "Migrating from 1.x" section below.

### Changed
- **[Breaking]** Authentication configuration is now sourced from `ballerinax/aws.auth` instead of being
  defined locally by this package. The `ConnectionConfig.auth` field type changed from
  `StaticAuthConfig|EC2IAMRoleConfig` to `auth:AuthConfig`. This is a widening — static credentials remain
  supported unchanged, with six additional credential sources added.
- **[Breaking]** The `ConnectionConfig.region` field type changed from `redshiftdata:Region` to
  `aws:Region|string`. The `string` alternative allows regions that are not yet present in the `aws:Region`
  enum to be supplied directly.
- **[Breaking]** `Client.close()` is now a normal method rather than a remote method, so it is invoked as
  `client.close()` instead of `client->close()`. Closing the client does not send a request to the Redshift
  Data API, so it does not warrant a remote method.
- Temporary credentials (STS assume-role, SSO, container and instance profiles) are now refreshed
  transparently by the credential provider, instead of the connector holding a single set of keys resolved
  at initialization time.
- The package now requires Ballerina distribution `2201.12.0` (was `2201.11.0`).

### Removed
- **[Breaking]** `redshiftdata:StaticAuthConfig` has been removed in favour of `auth:StaticAuthConfig`. The
  replacement record is structurally identical to the one it replaces, so inline record literals continue to
  work unchanged — only explicit type references need updating.
- **[Breaking]** `redshiftdata:EC2IAMRoleConfig` has been removed. EC2 instance profile credentials are now
  resolved through `auth:DEFAULT_CREDENTIALS`, which ends at the EC2 instance metadata service (IMDS).
- **[Breaking]** The `redshiftdata:Region` enum has been removed in favour of `aws:Region`.

### Added
- Support for six additional AWS credential sources, available through `auth:AuthConfig`:
  - `auth:ProfileAuthConfig` — credentials read from a named profile in the shared credentials file.
  - `auth:AssumeRoleConfig` — temporary credentials obtained by assuming an IAM role via AWS STS.
  - `auth:WebIdentityConfig` — web identity (OIDC) federation, including IAM Roles for Service Accounts (IRSA).
  - `auth:SsoAuthConfig` — AWS IAM Identity Center (SSO).
  - `auth:ProcessAuthConfig` — credentials sourced from an external credential process.
  - `auth:DEFAULT_CREDENTIALS` — the AWS default credential provider chain.
- A new optional `ConnectionConfig.endpoint` field of type `aws:EndpointConfig`, for selecting FIPS or
  dualstack endpoint variants and for overriding the endpoint entirely (for example, LocalStack or VPC
  interface endpoints).
- New `aws:Region` members not present in the former `redshiftdata:Region` enum: `AP_EAST_2`,
  `AP_SOUTHEAST_5`, `AP_SOUTHEAST_6`, `AP_SOUTHEAST_7`, `AWS_ISO_E_GLOBAL`, `AWS_ISO_F_GLOBAL`,
  `EUSC_DE_EAST_1`, `MX_CENTRAL_1`, `US_ISOB_WEST_1`, `US_ISOF_EAST_1` and `US_ISOF_SOUTH_1`. Every member of
  the former enum is present in `aws:Region` under the same name.

### Migrating from 1.x

Add an `import ballerinax/aws;` alongside the existing Redshift Data import, and qualify region members with
`aws:` rather than `redshiftdata:`. Static authentication record literals do not need to change:

```ballerina
// 1.x
import ballerinax/aws.redshiftdata;

redshiftdata:ConnectionConfig config = {
    region: redshiftdata:US_EAST_2,
    auth: {accessKeyId, secretAccessKey},
    dbAccessConfig: {id: clusterId, database: databaseName, dbUser: dbUser}
};
```

```ballerina
// 2.0.0
import ballerinax/aws;
import ballerinax/aws.redshiftdata;

redshiftdata:ConnectionConfig config = {
    region: aws:US_EAST_2,
    auth: {accessKeyId, secretAccessKey},
    dbAccessConfig: {id: clusterId, database: databaseName, dbUser: dbUser}
};
```

Code that referred to `redshiftdata:StaticAuthConfig` by name must be updated to the `ballerinax/aws.auth`
equivalent:

```ballerina
// 1.x
redshiftdata:StaticAuthConfig authConfig = {accessKeyId, secretAccessKey};
```

```ballerina
// 2.0.0
import ballerinax/aws.auth;

auth:StaticAuthConfig authConfig = {accessKeyId, secretAccessKey};
```

Configurations that relied on `redshiftdata:EC2IAMRoleConfig` should use the default credential provider
chain, which resolves EC2 instance profile credentials from IMDS as its last step:

```ballerina
// 1.x
redshiftdata:ConnectionConfig config = {
    region: redshiftdata:US_EAST_2,
    auth: {profileName: "dev"}
};
```

```ballerina
// 2.0.0
import ballerinax/aws;
import ballerinax/aws.auth;

redshiftdata:ConnectionConfig config = {
    region: aws:US_EAST_2,
    auth: auth:DEFAULT_CREDENTIALS
};
```

To keep reading credentials from a named profile in the shared credentials file, use
`auth:ProfileAuthConfig` instead:

```ballerina
// 2.0.0
import ballerinax/aws;

redshiftdata:ConnectionConfig config = {
    region: aws:US_EAST_2,
    auth: {profileName: "dev"}
};
```

Calls to `close` must drop the remote-call arrow:

```ballerina
// 1.x
check redshift->close();
```

```ballerina
// 2.0.0
check redshift.close();
```

## [1.1.0] - 2025-03-03

### Changed
- Migrated the connector to Java 21.

## [1.0.0] - 2025-02-19

### Added
- Initial release of the Ballerina AWS Redshift Data connector.
