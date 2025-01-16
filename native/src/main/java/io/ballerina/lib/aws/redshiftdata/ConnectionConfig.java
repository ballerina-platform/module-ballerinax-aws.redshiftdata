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

import io.ballerina.runtime.api.values.BMap;
import io.ballerina.runtime.api.values.BString;
import software.amazon.awssdk.regions.Region;

/**
 * {@code ConnectionConfig} represents the connection configuration required for
 * ballerina Redshift Data Client.
 *
 * @param region         The AWS region where the Redshift cluster is located.
 * @param authConfig     The authentication configuration required for the
 *                       Redshift Data Client.
 * @param dbAccessConfig The database access configurations for the Redshift Data API.
 */
public record ConnectionConfig(Region region, AuthConfig authConfig, Object dbAccessConfig) {

    public ConnectionConfig(BMap<BString, Object> bConnectionConfig) {
        this(
                getRegion(bConnectionConfig),
                getAuthConfig(bConnectionConfig),
                getDbAccessConfig(bConnectionConfig)
        );
    }

    private static Region getRegion(BMap<BString, Object> bConnectionConfig) {
        String regionStr = bConnectionConfig.getStringValue(Constants.CONNECTION_CONFIG_REGION).getValue();
        return Region.of(regionStr);
    }

    @SuppressWarnings("unchecked")
    private static AuthConfig getAuthConfig(BMap<BString, Object> bConnectionConfig) {
        BMap<BString, Object> bAuthConfig = (BMap<BString, Object>) bConnectionConfig
                .getMapValue(Constants.CONNECTION_CONFIG_AUTH_CONFIG);
        return new AuthConfig(bAuthConfig);
    }

    @SuppressWarnings("unchecked")
    private static Object getDbAccessConfig(BMap<BString, Object> bConnectionConfig) {
        if (bConnectionConfig.containsKey(Constants.CONNECTION_CONFIG_DB_ACCESS_CONFIG)) {
            BMap<BString, Object> bDbAccessConfig = (BMap<BString, Object>) bConnectionConfig
                    .get(Constants.CONNECTION_CONFIG_DB_ACCESS_CONFIG);
            if (bDbAccessConfig.containsKey(Constants.CLUSTER_ID)) {
                return new Cluster(bDbAccessConfig);
            }
            return new WorkGroup(bDbAccessConfig);
        }
        return null;
    }
}
