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
import software.amazon.awssdk.regions.Region;

import static io.ballerina.lib.aws.redshiftdata.Cluster.CLUSTER_ID;
import static io.ballerina.lib.aws.redshiftdata.StaticAuthConfig.AWS_ACCESS_KEY_ID;

/**
 * {@code ConnectionConfig} represents the connection configuration required for
 * ballerina Redshift Data API Client.
 *
 * @param region         The AWS region where the Redshift cluster is located.
 * @param authConfig     The authentication configuration required for the
 *                       Redshift Data API Client.
 * @param dbAccessConfig The database access configurations for the Redshift Data API.
 */
public record ConnectionConfig(Region region, Object authConfig, Object dbAccessConfig) {
    static final BString CONNECTION_CONFIG_DB_ACCESS_CONFIG = StringUtils.fromString("dbAccessConfig");
    private static final BString CONNECTION_CONFIG_REGION = StringUtils.fromString("region");
    private static final BString CONNECTION_CONFIG_AUTH_CONFIG = StringUtils.fromString("auth");

    public ConnectionConfig(BMap<BString, Object> bConnectionConfig) {
        this(
                getRegion(bConnectionConfig),
                getAuthConfig(bConnectionConfig),
                getDbAccessConfig(bConnectionConfig)
        );
    }

    private static Region getRegion(BMap<BString, Object> bConnectionConfig) {
        return Region.of(bConnectionConfig.getStringValue(CONNECTION_CONFIG_REGION).getValue());
    }

    @SuppressWarnings("unchecked")
    private static Object getAuthConfig(BMap<BString, Object> bConnectionConfig) {
        BMap<BString, Object> bAuthConfig = (BMap<BString, Object>) bConnectionConfig
                .getMapValue(CONNECTION_CONFIG_AUTH_CONFIG);
        if (bAuthConfig.containsKey(AWS_ACCESS_KEY_ID)) {
            return new StaticAuthConfig(bAuthConfig);
        }
        return new InstanceProfileCredentials(bAuthConfig);
    }

    @SuppressWarnings("unchecked")
    private static Object getDbAccessConfig(BMap<BString, Object> bConnectionConfig) {
        if (bConnectionConfig.containsKey(CONNECTION_CONFIG_DB_ACCESS_CONFIG)) {
            BMap<BString, Object> bDbAccessConfig = (BMap<BString, Object>) bConnectionConfig
                    .get(CONNECTION_CONFIG_DB_ACCESS_CONFIG);
            if (bDbAccessConfig.containsKey(CLUSTER_ID)) {
                return new Cluster(bDbAccessConfig);
            }
            return new WorkGroup(bDbAccessConfig);
        }
        return null;
    }
}
