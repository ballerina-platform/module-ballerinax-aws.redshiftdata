/*
 * Copyright (c) 2024, WSO2 LLC. (http://www.wso2.org).
 *
 * WSO2 LLC. licenses this file to you under the Apache License,
 * Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.ballerina.lib.aws.redshiftdata;

import io.ballerina.runtime.api.values.BMap;
import io.ballerina.runtime.api.values.BString;

/**
 * {@code WorkGroup} contains the java representation of the ballerina redshift data api workgroup.
 *
 * @param name                    The workgroup name or Amazon Resource Name (ARN).
 * @param database                The name of the database.
 * @param secretArn               The name or ARN of the secret that enables access to the database.
 * @param sessionKeepAliveSeconds The number of seconds to keep the session alive after the query finishes.
 */
public record WorkGroup(String name, String database, String secretArn, Integer sessionKeepAliveSeconds) {

    public WorkGroup(BMap<BString, Object> bWorkGroup) {
        this(
                getName(bWorkGroup),
                getDatabase(bWorkGroup),
                getSecretArn(bWorkGroup),
                getSessionKeepAliveSeconds(bWorkGroup)
        );
    }

    private static String getName(BMap<BString, Object> bWorkGroup) {
        if (bWorkGroup.containsKey(Constants.WORK_GROUP_NAME)) {
            return bWorkGroup.getStringValue(Constants.WORK_GROUP_NAME).getValue();
        }
        return null;
    }

    private static String getDatabase(BMap<BString, Object> bWorkGroup) {
        if (bWorkGroup.containsKey(Constants.WORK_GROUP_DATABASE)) {
            return bWorkGroup.getStringValue(Constants.WORK_GROUP_DATABASE).getValue();
        }
        return null;
    }

    private static String getSecretArn(BMap<BString, Object> bWorkGroup) {
        if (bWorkGroup.containsKey(Constants.WORK_GROUP_SECRET_ARN)) {
            return bWorkGroup.getStringValue(Constants.WORK_GROUP_SECRET_ARN).getValue();
        }
        return null;
    }

    private static Integer getSessionKeepAliveSeconds(BMap<BString, Object> bWorkGroup) {
        if (bWorkGroup.containsKey(Constants.WORK_GROUP_SESSION_KEEP_ALIVE_SECONDS)) {
            return Integer.parseInt(bWorkGroup.getStringValue(
                    Constants.WORK_GROUP_SESSION_KEEP_ALIVE_SECONDS).getValue());
        }
        return null;
    }
}
