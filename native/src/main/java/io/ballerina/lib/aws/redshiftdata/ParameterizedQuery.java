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
import io.ballerina.runtime.api.values.BArray;
import io.ballerina.runtime.api.values.BObject;
import io.ballerina.runtime.api.values.BString;
import software.amazon.awssdk.services.redshiftdata.model.SqlParameter;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * Represents a parameterized SQL query for use with AWS Redshift Data API.
 * <p>
 * This class facilitates constructing a query with named parameters and converting
 * it into a format compatible with the AWS SDK for Java's Redshift Data API.
 * It manages the query strings and their associated parameter values, allowing
 * for efficient preparation and execution of parameterized queries.
 * </p>
 * <p>
 * The query is built using two components:
 * <ul>
 *   <li><b>Query Strings:</b> Static parts of the query that remain constant.</li>
 *   <li><b>Insertions:</b> Dynamic parts of the query represented as named parameters.</li>
 * </ul>
 * </p>
 * <p>
 * Example:
 * <pre>
 * BObject bSqlStatement = // Ballerina object with query details
 * ParameterizedQuery query = new ParameterizedQuery(bSqlStatement);
 *
 * // Get the query string with named parameters
 * String queryString = query.getQueryString();
 *
 * // Get the fully prepared query string with values substituted
 * String preparedQuery = query.getPreparedQuery();
 *
 * // Get the list of parameters for the Redshift Data API
 * SqlParameter[] parameters = query.getParameters();
 * </pre>
 *
 * <p>
 * This class also provides utility methods to check if the query contains any parameters.
 * </p>
 */
public class ParameterizedQuery {
    private static final BString QUERY_STRINGS = StringUtils.fromString("strings");
    private static final BString QUERY_INSERTIONS = StringUtils.fromString("insertions");
    private final String[] strings;
    private final String[] insertions;

    /**
     * Constructs a ParameterizedQuery instance from a Ballerina object.
     *
     * @param bSqlStatement the Ballerina object containing query strings and insertions
     */
    public ParameterizedQuery(BObject bSqlStatement) {
        String[] strings = bSqlStatement.getArrayValue(QUERY_STRINGS).getStringArray();
        BArray bInsertions = bSqlStatement.getArrayValue(QUERY_INSERTIONS);
        List<String> insertions = new ArrayList<>();
        for (int i = 0; i < bInsertions.size(); i++) {
            Object value = bInsertions.get(i);
            // If the value is null, insert "NULL" to the query string
            if (Objects.isNull(value)) {
                strings[i] += "NULL";
            } else {
                insertions.add(value.toString());
            }
        }
        this.strings = strings;
        this.insertions = insertions.toArray(new String[0]);
    }

    /**
     * Constructs the query string with named parameters.
     * <p>
     * Each parameter is represented as a placeholder in the form <code>:paramN</code>,
     * where <code>N</code> is the index of the parameter.
     * </p>
     *
     * @return the constructed query string with named placeholders
     */
    public String getQueryString() {
        StringBuilder query = new StringBuilder();
        for (int i = 0; i < strings.length; i++) {
            query.append(strings[i]);
            if (i < insertions.length) {
                query.append(":param").append(i);
            }
        }
        return query.toString();
    }

    /**
     * Constructs the prepared query string with parameter values directly inserted.
     * <p>
     * This replaces the placeholders with the actual parameter values, resulting in
     * a fully substituted query string. This is useful for debugging but should not
     * be used directly for execution to avoid SQL injection risks.
     * </p>
     *
     * @return the fully substituted query string
     */
    public String getPreparedQuery() {
        StringBuilder query = new StringBuilder();
        for (int i = 0; i < strings.length; i++) {
            query.append(strings[i]);
            if (i < insertions.length) {
                query.append("'").append(insertions[i]).append("'");
            }
        }
        return query.toString();
    }

    /**
     * Converts the parameter values into an array of SqlParameter objects.
     * <p>
     * Each parameter is named in the format <code>paramN</code> and mapped to its
     * corresponding value. These SqlParameter objects can be used with the Redshift
     * Data API for parameterized query execution.
     * </p>
     *
     * @return an array of SqlParameter objects representing the query parameters
     */
    public SqlParameter[] getParameters() {
        SqlParameter[] parameters = new SqlParameter[insertions.length];
        for (int i = 0; i < insertions.length; i++) {
            parameters[i] = SqlParameter.builder().name("param" + i).value(insertions[i]).build();
        }
        return parameters;
    }

    /**
     * Checks if the query contains any parameters.
     *
     * @return <code>true</code> if the query has parameters; <code>false</code> otherwise
     */
    public boolean hasParameters() {
        return insertions.length > 0;
    }
}
