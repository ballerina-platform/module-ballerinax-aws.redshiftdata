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

import io.ballerina.runtime.api.PredefinedTypes;
import io.ballerina.runtime.api.creators.TypeCreator;
import io.ballerina.runtime.api.creators.ValueCreator;
import io.ballerina.runtime.api.types.RecordType;
import io.ballerina.runtime.api.utils.StringUtils;
import io.ballerina.runtime.api.utils.TypeUtils;
import io.ballerina.runtime.api.values.BMap;
import io.ballerina.runtime.api.values.BObject;
import io.ballerina.runtime.api.values.BStream;
import io.ballerina.runtime.api.values.BString;
import io.ballerina.runtime.api.values.BTypedesc;
import software.amazon.awssdk.services.redshiftdata.RedshiftDataClient;
import software.amazon.awssdk.services.redshiftdata.model.ColumnMetadata;
import software.amazon.awssdk.services.redshiftdata.model.Field;
import software.amazon.awssdk.services.redshiftdata.model.GetStatementResultRequest;
import software.amazon.awssdk.services.redshiftdata.model.GetStatementResultResponse;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Represents the utility functions for processing query results.
 */
public class QueryResultProcessor {
    private QueryResultProcessor() {
    }

    public static BStream getRecordStream(RedshiftDataClient nativeClient, String statementId,
                                          GetStatementResultResponse nativeResultResponse, BTypedesc recordType) {
        try {
            List<ColumnMetadata> columnMetadata = nativeResultResponse.columnMetadata();
            RecordType streamConstraint = (RecordType) TypeUtils.getReferredType(
                    recordType.getDescribingType());

            List<String> resultFields = columnMetadata.stream().map(ColumnMetadata::name).toList();
            String[] ballerinaFields = streamConstraint.getFields().keySet().toArray(new String[0]);

            // Map the field name with result column index
            Map<String, Integer> columnIndexMap = new HashMap<>();
            int columnIndex = -1;
            for (String ballerinaField : ballerinaFields) {
                columnIndex = resultFields.indexOf(ballerinaField);
                if (columnIndex == -1) {
                    throw new RuntimeException("Field '" + ballerinaField + "' not found in the result set.");
                }
                columnIndexMap.put(ballerinaField, columnIndex);
            }

            BObject resultIterator = ValueCreator.createObjectValue(ModuleUtils.getModule(),
                    Constants.RESULT_ITERATOR_OBJECT);
            resultIterator.addNativeData(Constants.RESULT_ITERATOR_RESULT_RESPONSE, nativeResultResponse);
            resultIterator.addNativeData(Constants.RESULT_ITERATOR_CURRENT_RESULT_INDEX, 0L);
            resultIterator.addNativeData(Constants.RESULT_ITERATOR_COLUMN_INDEX_MAP, columnIndexMap);
            resultIterator.addNativeData(Constants.RESULT_ITERATOR_RECORD_TYPE, streamConstraint);
            // Add additional data for fetching the next result set
            resultIterator.addNativeData(Constants.RESULT_ITERATOR_STATEMENT_ID, statementId);
            resultIterator.addNativeData(Constants.NATIVE_CLIENT, nativeClient);

            return ValueCreator.createStreamValue(TypeCreator.createStreamType(streamConstraint,
                    PredefinedTypes.TYPE_NULL), resultIterator);
        } catch (Exception e) {
            throw new RuntimeException("Error occurred while creating the Record Stream: "
                    + Objects.requireNonNullElse(e.getMessage(), "Unknown error"));
        }
    }

    @SuppressWarnings("unchecked")
    public static Object nextResult(BObject bResultIterator) {
        RecordType recordType = (RecordType) bResultIterator.getNativeData(Constants.RESULT_ITERATOR_RECORD_TYPE);
        long index = (long) bResultIterator.getNativeData(Constants.RESULT_ITERATOR_CURRENT_RESULT_INDEX);
        Map<String, Integer> columnIndexMap = (Map<String, Integer>) bResultIterator
                .getNativeData(Constants.RESULT_ITERATOR_COLUMN_INDEX_MAP);
        GetStatementResultResponse resultResponse = (GetStatementResultResponse) bResultIterator
                .getNativeData(Constants.RESULT_ITERATOR_RESULT_RESPONSE);

        List<List<Field>> rows = resultResponse.records();
        try {
            // Fetch the next record when the current result set is processed
            if (index >= rows.size()) {
                if (Objects.nonNull(resultResponse.nextToken())) {
                    RedshiftDataClient nativeClient = (RedshiftDataClient) bResultIterator
                            .getNativeData(Constants.RESULT_ITERATOR_NATIVE_CLIENT);
                    String statementId = (String) bResultIterator
                            .getNativeData(Constants.RESULT_ITERATOR_STATEMENT_ID);

                    resultResponse = nativeClient.getStatementResult(
                            GetStatementResultRequest.builder()
                                    .id(statementId).nextToken(resultResponse.nextToken()).build());
                    rows = resultResponse.records();
                    index = 0;
                    bResultIterator.addNativeData(Constants.RESULT_ITERATOR_CURRENT_RESULT_INDEX, index);
                    bResultIterator.addNativeData(Constants.RESULT_ITERATOR_RESULT_RESPONSE, resultResponse);
                }
            }

            if (index < rows.size()) {
                List<Field> row = rows.get((int) index);
                BMap<BString, Object> record = ValueCreator.createRecordValue(recordType.getPackage(),
                        recordType.getName());

                for (String fieldName : columnIndexMap.keySet()) {
                    int columnIndex = columnIndexMap.get(fieldName);
                    Field field = row.get(columnIndex);
                    record.put(StringUtils.fromString(fieldName), getFieldValue(field));
                }
                bResultIterator.addNativeData(Constants.RESULT_ITERATOR_CURRENT_RESULT_INDEX, index + 1);
                return record;
            }
            closeResult(bResultIterator);
            return null;
        } catch (Exception e) {
            closeResult(bResultIterator);
            String errorMsg = String.format("Error occurred while iterating the Query result: %s",
                    Objects.requireNonNullElse(e.getMessage(), "Unknown error"));
            return CommonUtils.createError(errorMsg, e);
        }
    }

    private static Object getFieldValue(Field field) {
        if (field.isNull() != null && field.isNull()) {
            return null;
        } else if (field.stringValue() != null) {
            return StringUtils.fromString(field.stringValue());
        } else if (field.booleanValue() != null) {
            return field.booleanValue();
        } else if (field.longValue() != null) {
            return field.longValue();
        } else if (field.doubleValue() != null) {
            return field.doubleValue();
        }
        return null;
    }

    public static void closeResult(BObject recordIterator) {
        try {
            recordIterator.addNativeData(Constants.RESULT_ITERATOR_RESULT_RESPONSE, null);
            recordIterator.addNativeData(Constants.RESULT_ITERATOR_RECORD_TYPE, null);
            recordIterator.addNativeData(Constants.RESULT_ITERATOR_CURRENT_RESULT_INDEX, null);
            recordIterator.addNativeData(Constants.RESULT_ITERATOR_COLUMN_INDEX_MAP, null);
            recordIterator.addNativeData(Constants.RESULT_ITERATOR_NATIVE_CLIENT, null);
            recordIterator.addNativeData(Constants.RESULT_ITERATOR_STATEMENT_ID, null);
        } catch (Exception e) {
            throw new RuntimeException("Error occurred while closing the Query result: " + e.getMessage());
        }
    }
}
