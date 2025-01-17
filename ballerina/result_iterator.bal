// Copyright (c) 2025, WSO2 LLC. (http://www.wso2.com).
//
// WSO2 LLC. licenses this file to you under the Apache License,
// Version 2.0 (the "License"); you may not use this file except
// in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

import ballerina/jballerina.java;

# The result iterator used to iterate results in stream returned from `getQueryResult` method.
isolated class ResultIterator {
    private boolean isClosed = false;

    public isolated function next() returns record {|record {} value;|}|Error? {
        boolean closed;
        lock {
            closed = self.isClosed;
        }
        if closed {
            return error Error("Stream is closed. Therefore, no operations are allowed further on the stream.");
        }
        record {}|Error? result = self.externNextResult(self);
        if result is record {} {
            record {|
                record {} value;
            |} streamRecord = {value: result};
            return streamRecord;
        } else if result is Error {
            lock {
                self.isClosed = true;
            }
            return result;
        } else {
            lock {
                self.isClosed = true;
            }
            return result;
        }
    }

    isolated function externNextResult(ResultIterator iterator) returns record {}|Error? = @java:Method {
        name: "nextResult",
        'class: "io.ballerina.lib.aws.redshiftdata.QueryResultProcessor"
    } external;

    public isolated function close() returns Error? {
        boolean closed;
        lock {
            closed = self.isClosed;
        }
        if !closed {
            Error? e = self.externCloseResult(self);
            if e is () {
                lock {
                    self.isClosed = true;
                }
            }
            return e;
        }
    }

    isolated function externCloseResult(ResultIterator iterator) returns Error? = @java:Method {
        name: "closeResult",
        'class: "io.ballerina.lib.aws.redshiftdata.QueryResultProcessor"
    } external;
}
