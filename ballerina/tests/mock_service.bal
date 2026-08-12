//  Copyright (c) 2025, WSO2 LLC. (http://www.wso2.org).
//
//  WSO2 LLC. licenses this file to you under the Apache License,
//  Version 2.0 (the "License"); you may not use this file except
//  in compliance with the License.
//  You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
//  Unless required by applicable law or agreed to in writing,
//  software distributed under the License is distributed on an
//  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
//  KIND, either express or implied. See the License for the
//  specific language governing permissions and limitations
//  under the License.

// A mock of the Amazon Redshift Data API operations used by this connector.
import ballerina/http;
import ballerina/time;

const int MOCK_SERVICE_PORT = 9090;
const string TARGET_PREFIX = "RedshiftData.";

// The endpoint the tests point their clients at, through `endpointConfig`.
final string mockServiceUrl = string `http://localhost:${MOCK_SERVICE_PORT}`;

// The number of records in a single `GetStatementResult` page; the rest is served
// through `NextToken`, the way the service paginates.
const int RESULT_PAGE_SIZE = 500;

// Statement metadata the tests only assert to be positive.
const int REDSHIFT_PID = 1073741823;
const int REDSHIFT_QUERY_ID = 262144;
const int DURATION_NANOS = 123456789;

const string STATEMENT_ERROR = "ERROR: relation \"non_existent_table\" does not exist";

// The identifier format the Redshift Data API accepts for a statement.
final string:RegExp STATEMENT_ID_PATTERN =
    re `[a-z0-9]{8}-[a-z0-9]{4}-[a-z0-9]{4}-[a-z0-9]{4}-[a-z0-9]{12}(:\d+)?`;

service on new http:Listener(MOCK_SERVICE_PORT) {

    isolated resource function post .(@http:Header {name: "X-Amz-Target"} string target, http:Request request)
            returns json|http:BadRequest|error {
        map<json> payload = check parsePayload(request);
        match target.substring(TARGET_PREFIX.length()) {
            "ExecuteStatement" => {
                return executeStatement(payload);
            }
            "BatchExecuteStatement" => {
                return batchExecuteStatement(payload);
            }
            "DescribeStatement" => {
                return describeStatement(payload);
            }
            "GetStatementResult" => {
                return getStatementResult(payload);
            }
        }
        return awsError("UnknownOperationException", string `Unsupported operation: ${target}`);
    }
}

// ===== Operations =====

isolated function executeStatement(map<json> payload) returns json|http:BadRequest {
    http:BadRequest? accessError = validateDbAccess(payload);
    if accessError is http:BadRequest {
        return accessError;
    }
    string sql = stringValue(payload, "Sql") ?: "";
    return executionResponse(newStatement(nextStatementId(), sql, statementKindOf(sql), sessionOf(payload)));
}

isolated function batchExecuteStatement(map<json> payload) returns json|http:BadRequest {
    http:BadRequest? accessError = validateDbAccess(payload);
    if accessError is http:BadRequest {
        return accessError;
    }
    json sqls = payload["Sqls"];
    if sqls !is json[] || sqls.length() == 0 {
        return validationError("Sqls must have at least 1 item.");
    }

    // The sub-statement identifiers are derived from the batch identifier, as the
    // service does.
    string batchId = nextStatementId();
    MockStatement[] subStatements = [];
    foreach int index in 0 ..< sqls.length() {
        json sql = sqls[index];
        string sqlText = sql is string ? sql : "";
        subStatements.push(newStatement(string `${batchId}:${index + 1}`, sqlText, statementKindOf(sqlText), ()));
    }
    return executionResponse(newStatement(batchId, "", BATCH, sessionOf(payload), subStatements));
}

isolated function describeStatement(map<json> payload) returns json|http:BadRequest {
    string statementId = stringValue(payload, "Id") ?: "";
    if !STATEMENT_ID_PATTERN.isFullMatch(statementId) {
        return invalidStatementIdError();
    }
    MockStatement? statement = lookupStatement(statementId);
    if statement is () {
        return validationError("Query does not exist.");
    }

    map<json> response = statementData(statement);
    response["RedshiftPid"] = REDSHIFT_PID;
    string? sessionId = statement.sessionId;
    if sessionId is string {
        response["SessionId"] = sessionId;
    }
    if statement.kind == BATCH {
        response["SubStatements"] = from MockStatement subStatement in statement.subStatements
            select statementData(subStatement);
    }
    return response;
}

isolated function getStatementResult(map<json> payload) returns json|http:BadRequest {
    string statementId = stringValue(payload, "Id") ?: "";
    if !STATEMENT_ID_PATTERN.isFullMatch(statementId) {
        return invalidStatementIdError();
    }
    MockStatement? statement = lookupStatement(statementId);
    if statement is () {
        return validationError("Query does not exist.");
    }
    MockResultSet? result = resultOf(statement.kind);
    if result is () {
        return validationError("Query does not have result. " +
                "Please check query status with DescribeStatement.");
    }

    int offset = 0;
    string? nextToken = stringValue(payload, "NextToken");
    if nextToken is string {
        int|error pageStart = int:fromString(nextToken);
        if pageStart is error {
            return validationError("Invalid pagination token.");
        }
        offset = pageStart;
    }
    int totalRows = result.rows.length();
    int pageEnd = int:min(offset + RESULT_PAGE_SIZE, totalRows);

    map<json> response = {
        "ColumnMetadata": from MockColumn column in result.columns
            select {"name": column.name, "label": column.name, "typeName": column.typeName, "nullable": 1},
        "Records": from int rowIndex in offset ..< pageEnd
            select from MockValue value in result.rows[rowIndex]
                select fieldValue(value),
        "TotalNumRows": totalRows
    };
    if pageEnd < totalRows {
        response["NextToken"] = pageEnd.toString();
    }
    return response;
}

// ===== Responses =====

isolated function executionResponse(MockStatement statement) returns json {
    map<json> response = {"Id": statement.id, "CreatedAt": statement.createdAt};
    string? sessionId = statement.sessionId;
    if sessionId is string {
        response["SessionId"] = sessionId;
    }
    return response;
}

isolated function statementData(MockStatement statement) returns map<json> {
    StatementKind kind = statement.kind;
    boolean batch = kind == BATCH;
    MockStatement[] subStatements = statement.subStatements;
    boolean failed = batch ? subStatements.some(sub => sub.kind == FAILING) : kind == FAILING;
    MockResultSet? result = resultOf(kind);
    int rows = result is MockResultSet ? result.rows.length() : 0;

    map<json> statementData = {
        "Id": statement.id,
        "CreatedAt": statement.createdAt,
        "UpdatedAt": statement.createdAt,
        "Status": failed ? FAILED : FINISHED,
        "HasResultSet": batch ? subStatements.some(sub => resultOf(sub.kind) is MockResultSet) :
            result is MockResultSet,
        // A batch statement reports no query identifier or result counts of its
        // own; those belong to its sub-statements.
        "RedshiftQueryId": batch || failed ? 0 : REDSHIFT_QUERY_ID,
        "ResultRows": batch ? -1 : rows,
        "ResultSize": batch ? -1 : resultSizeOf(kind, rows),
        "Duration": DURATION_NANOS
    };
    if statement.queryString != "" {
        statementData["QueryString"] = statement.queryString;
    }
    if failed {
        statementData["Error"] = STATEMENT_ERROR;
    }
    return statementData;
}

isolated function fieldValue(MockValue value) returns json {
    if value is () {
        return {"isNull": true};
    }
    if value is string {
        return {"stringValue": value};
    }
    if value is boolean {
        return {"booleanValue": value};
    }
    if value is int {
        return {"longValue": value};
    }
    return {"doubleValue": value};
}

// The cluster has to be reachable from the region the request was signed for.
isolated function validateDbAccess(map<json> payload) returns http:BadRequest? {
    if stringValue(payload, "SessionId") is string || stringValue(payload, "WorkgroupName") is string {
        return ();
    }
    string? clusterIdentifier = stringValue(payload, "ClusterIdentifier");
    if clusterIdentifier is () {
        return validationError("Either ClusterIdentifier, WorkgroupName or SessionId must be provided.");
    }
    if clusterIdentifier != clusterId {
        return validationError("Redshift endpoint doesn't exist in this region.");
    }
    return ();
}

// The session a statement runs in: the requested one, or a newly started session
// when the request asks to keep one alive.
isolated function sessionOf(map<json> payload) returns string? {
    string? sessionId = stringValue(payload, "SessionId");
    if sessionId is string {
        return sessionId;
    }
    return payload["SessionKeepAliveSeconds"] is int ? nextIdentifier(SESSION_ID_PREFIX) : ();
}

// Errors carry the exception name in the `x-amzn-errortype` header and the
// `__type`/`message` members, the shape the AWS JSON protocol expects.
isolated function awsError(string errorType, string message) returns http:BadRequest => {
    headers: {"x-amzn-errortype": errorType},
    body: {"__type": errorType, "message": message}
};

isolated function validationError(string message) returns http:BadRequest =>
    awsError("ValidationException", message);

isolated function invalidStatementIdError() returns http:BadRequest =>
    validationError("id must satisfy regex pattern: " +
            "^[a-z0-9]{8}-[a-z0-9]{4}-[a-z0-9]{4}-[a-z0-9]{4}-[a-z0-9]{12}(:\\d+)?$");

// ===== Canned statement results =====

// What the service reports for a statement, one member per query the test suite
// submits. A query none of the cases in `statementKindOf` match selects all users.
enum StatementKind {
    // `CREATE TABLE`, `INSERT INTO` and `DROP TABLE`: finish without a result set
    NO_RESULT_SET,
    ALL_USERS,
    SINGLE_USER,
    NO_ROWS,
    SUPPORTED_TYPES,
    PAGINATED,
    // A statement the cluster rejects
    FAILING,
    // The parent of a batch execution, which reports on its sub-statements
    BATCH
}

type MockColumn record {|
    string name;
    string typeName;
|};

// A column value, limited to the types the connector maps back to Ballerina.
type MockValue string|int|float|boolean|();

type MockResultSet record {|
    MockColumn[] columns;
    MockValue[][] rows;
|};

// The rows `beforeTestSuite` inserts into `Users`.
final readonly & MockColumn[] USER_COLUMNS = [
    {name: "user_id", typeName: "int4"},
    {name: "username", typeName: "varchar"},
    {name: "email", typeName: "varchar"},
    {name: "age", typeName: "int4"}
];
final readonly & MockValue[][] USER_ROWS = [
    [1, "JohnDoe", "john.doe@example.com", 25],
    [2, "JaneSmith", "jane.smith@example.com", 30],
    [3, "BobJohnson", "bob.johnson@example.com", 22]
];

// The row `testSupportedTypes` inserts into `SupportedTypes`.
final readonly & MockColumn[] SUPPORTED_TYPE_COLUMNS = [
    {name: "int_type", typeName: "int4"},
    {name: "bigint_type", typeName: "int8"},
    {name: "double_type", typeName: "float8"},
    {name: "boolean_type", typeName: "bool"},
    {name: "string_type", typeName: "varchar"},
    {name: "nil_type", typeName: "varchar"}
];
final readonly & MockValue[][] SUPPORTED_TYPE_ROWS = [
    [12, 9223372036854774807, 123.34, true, "test", ()]
];

const int PAGINATION_ROW_COUNT = 1601;
const int PAGINATION_COLUMN_LENGTH = 100000;

isolated function statementKindOf(string sql) returns StatementKind {
    string query = sql.trim().toUpperAscii();
    if !query.startsWith("SELECT") {
        return NO_RESULT_SET;
    }
    if query.includes("LARGE_COLUMN") {
        return PAGINATED;
    }
    if query.includes("NON_EXISTENT_TABLE") {
        return FAILING;
    }
    if query.includes("SUPPORTEDTYPES") {
        return SUPPORTED_TYPES;
    }
    if query.includes("USER_ID = 0") {
        return NO_ROWS;
    }
    if query.includes("WHERE") {
        return SINGLE_USER;
    }
    return ALL_USERS;
}

isolated function resultOf(StatementKind kind) returns MockResultSet? {
    match kind {
        ALL_USERS => {
            return {columns: USER_COLUMNS, rows: USER_ROWS};
        }
        SINGLE_USER => {
            return {columns: USER_COLUMNS, rows: [USER_ROWS[0]]};
        }
        NO_ROWS => {
            return {columns: USER_COLUMNS, rows: []};
        }
        SUPPORTED_TYPES => {
            return {columns: SUPPORTED_TYPE_COLUMNS, rows: SUPPORTED_TYPE_ROWS};
        }
        PAGINATED => {
            return {
                columns: [{name: "num", typeName: "int4"}, {name: "large_column", typeName: "varchar"}],
                rows: from int number in 0 ..< PAGINATION_ROW_COUNT
                    select [number, "X"]
            };
        }
    }
    return ();
}

isolated function resultSizeOf(StatementKind kind, int rows) returns int =>
    kind == PAGINATED ? rows * PAGINATION_COLUMN_LENGTH : rows * 64;

// ===== Statements in flight =====

type MockStatement record {|
    string id;
    // The statement as submitted, which `DescribeStatement` reports back. Empty
    // for a batch parent, which has no query of its own
    string queryString;
    StatementKind kind;
    string? sessionId;
    MockStatement[] subStatements;
    int createdAt;
|};

// Prefixes that, with a sequence number, produce identifiers in the format the
// Redshift Data API uses.
const string STATEMENT_ID_PREFIX = "3f0c7e2a-9b41-4d6e-8f5a-";
const string SESSION_ID_PREFIX = "5d2b18c4-6ea7-4f39-b0c1-";

isolated map<readonly & MockStatement> statements = {};
isolated int sequence = 0;

isolated function newStatement(string statementId, string queryString, StatementKind kind, string? sessionId,
        MockStatement[] subStatements = []) returns readonly & MockStatement {
    MockStatement statement = {
        id: statementId,
        queryString,
        kind,
        sessionId,
        subStatements,
        createdAt: time:utcNow()[0]
    };
    readonly & MockStatement recorded = statement.cloneReadOnly();
    lock {
        statements[recorded.id] = recorded;
    }
    return recorded;
}

isolated function lookupStatement(string statementId) returns (readonly & MockStatement)? {
    lock {
        return statements[statementId];
    }
}

isolated function nextStatementId() returns string => nextIdentifier(STATEMENT_ID_PREFIX);

isolated function nextIdentifier(string prefix) returns string {
    int next;
    lock {
        sequence += 1;
        next = sequence;
    }
    string suffix = next.toHexString().toLowerAscii();
    while suffix.length() < 12 {
        suffix = "0" + suffix;
    }
    return prefix + suffix;
}

// ===== Helpers =====

isolated function parsePayload(http:Request request) returns map<json>|error {
    byte[] body = check request.getBinaryPayload();
    json payload = body.length() == 0 ? {} : check (check string:fromBytes(body)).fromJsonString();
    return payload is map<json> ? payload : {};
}

isolated function stringValue(map<json> payload, string name) returns string? {
    json value = payload[name];
    return value is string ? value : ();
}
