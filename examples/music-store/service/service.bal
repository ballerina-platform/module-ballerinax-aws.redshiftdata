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

import ballerina/http;
import ballerina/lang.runtime;
import ballerinax/aws.redshiftdata;

configurable string accessKeyId = ?;
configurable string secretAccessKey = ?;
configurable redshiftdata:Cluster dbAccessConfig = ?;

type Album record {|
    string id;
    string title;
    string artist;
    float price;
|};

service / on new http:Listener(8080) {
    private final redshiftdata:Client redshift;

    function init() returns error? {
        self.redshift = check new ({
            region: redshiftdata:US_EAST_2,
            authConfig: {
                accessKeyId,
                secretAccessKey
            },
            dbAccessConfig
        });
    }

    resource function get albums() returns Album[]|error {
        redshiftdata:ExecutionResponse res = check self.redshift->executeStatement(`SELECT * FROM Albums`);
        _ = check waitForCompletion(self.redshift, res.statementId);
        stream<Album, redshiftdata:Error?> albumStream = check self.redshift->getResultAsStream(res.statementId);
        return from Album album in albumStream
            select album;
    }

    resource function get albums/[string id]() returns Album|http:NotFound|error {
        redshiftdata:ExecutionResponse res = check self.redshift->executeStatement(`SELECT * FROM Albums WHERE id = ${id}`);
        _ = check waitForCompletion(self.redshift, res.statementId);
        stream<Album, redshiftdata:Error?> albumStream = check self.redshift->getResultAsStream(res.statementId);
        Album[] albums = check from Album album in albumStream
            select album;
        if albums.length() == 0 {
            return http:NOT_FOUND;
        } else {
            return albums[0];
        }
    }

    resource function post album(@http:Payload Album album) returns Album|error {
        redshiftdata:ExecutionResponse res = check self.redshift->executeStatement(`
            INSERT INTO Albums (id, title, artist, price)
            VALUES (${album.id}, ${album.title}, ${album.artist}, ${album.price});`);
        redshiftdata:DescriptionResponse insertQueryDescribeStatement =
            check waitForCompletion(self.redshift, res.statementId);

        if insertQueryDescribeStatement.status == redshiftdata:FINISHED {
            return album;
        }
        return error("Failed to insert the album");
    }
}

isolated function waitForCompletion(redshiftdata:Client redshift, string statementId)
returns redshiftdata:DescriptionResponse|redshiftdata:Error {
    foreach int retryCount in 0 ... 9 {
        redshiftdata:DescriptionResponse descriptionResponse = check redshift->describe(statementId);
        if descriptionResponse.status is redshiftdata:FINISHED|redshiftdata:FAILED|redshiftdata:ABORTED {
            return descriptionResponse;
        }
        runtime:sleep(1);
    }
    return error("Statement execution did not finish within the expected time");
}
