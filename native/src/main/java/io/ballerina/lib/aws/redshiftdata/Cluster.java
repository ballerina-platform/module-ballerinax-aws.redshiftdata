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
 * {@code Cluster} contains the java representation of the ballerina redshift data api cluster.
 *
 * @param id                      The cluster identifier.
 * @param database                The name of the database.
 * @param dbUser                  The database username.
 * @param secretArn               The name or ARN of the secret that enables access to the database.
 * @param sessionKeepAliveSeconds The number of seconds to keep the session alive after the query finishes.
 */
public record Cluster(String id, String database, String dbUser, String secretArn, Integer sessionKeepAliveSeconds) {

    public Cluster(BMap<BString, Object> bCluster) {
        this(
                getId(bCluster),
                getDatabase(bCluster),
                getDbUser(bCluster),
                getSecretArn(bCluster),
                getSessionKeepAliveSeconds(bCluster)
        );
    }

    private static String getId(BMap<BString, Object> bCluster) {
        if (bCluster.containsKey(Constants.CLUSTER_ID)) {
            return bCluster.getStringValue(Constants.CLUSTER_ID).getValue();
        }
        return null;
    }

    private static String getDatabase(BMap<BString, Object> bCluster) {
        if (bCluster.containsKey(Constants.CLUSTER_DATABASE)) {
            return bCluster.getStringValue(Constants.CLUSTER_DATABASE).getValue();
        }
        return null;
    }

    private static String getDbUser(BMap<BString, Object> bCluster) {
        if (bCluster.containsKey(Constants.CLUSTER_DB_USER)) {
            return bCluster.getStringValue(Constants.CLUSTER_DB_USER).getValue();
        }
        return null;
    }

    private static String getSecretArn(BMap<BString, Object> bCluster) {
        if (bCluster.containsKey(Constants.CLUSTER_SECRET_ARN)) {
            return bCluster.getStringValue(Constants.CLUSTER_SECRET_ARN).getValue();
        }
        return null;
    }

    private static Integer getSessionKeepAliveSeconds(BMap<BString, Object> bCluster) {
        if (bCluster.containsKey(Constants.CLUSTER_SESSION_KEEP_ALIVE_SECONDS)) {
            return bCluster.getIntValue(Constants.CLUSTER_SESSION_KEEP_ALIVE_SECONDS).intValue();
        }
        return null;
    }
}
