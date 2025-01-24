/*
 * Copyright (c) 2025, WSO2 LLC. (http://www.wso2.org).
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

import io.ballerina.runtime.api.utils.StringUtils;
import io.ballerina.runtime.api.values.BMap;
import io.ballerina.runtime.api.values.BString;

/**
 * {@code Cluster} contains the java representation of the ballerina redshift data api cluster.
 *
 * @param id                      The cluster identifier.
 * @param database                The name of the database.
 * @param dbUser                  The database username.
 * @param secretArn               The name or ARN of the secret that enables access to the database.
 * @param sessionKeepAliveSeconds The number of seconds to keep the session alive after the query finishes.
 */
public record Cluster(String id, String database, String dbUser, String secretArn, Integer sessionKeepAliveSeconds) {
    static final BString CLUSTER_ID = StringUtils.fromString("id");
    private static final BString CLUSTER_DATABASE = StringUtils.fromString("database");
    private static final BString CLUSTER_DB_USER = StringUtils.fromString("dbUser");
    private static final BString CLUSTER_SECRET_ARN = StringUtils.fromString("secretArn");
    private static final BString CLUSTER_SESSION_KEEP_ALIVE_SECONDS = StringUtils.fromString("sessionKeepAliveSeconds");

    public Cluster(BMap<BString, Object> bCluster) {
        this(
                bCluster.getStringValue(CLUSTER_ID).getValue(),
                bCluster.getStringValue(CLUSTER_DATABASE).getValue(),
                bCluster.containsKey(CLUSTER_DB_USER) ?
                        bCluster.getStringValue(CLUSTER_DB_USER).getValue() : null,
                bCluster.containsKey(CLUSTER_SECRET_ARN) ?
                        bCluster.getStringValue(CLUSTER_SECRET_ARN).getValue() : null,
                bCluster.containsKey(CLUSTER_SESSION_KEEP_ALIVE_SECONDS) ?
                        bCluster.getIntValue(CLUSTER_SESSION_KEEP_ALIVE_SECONDS).intValue() : null
        );
    }
}
