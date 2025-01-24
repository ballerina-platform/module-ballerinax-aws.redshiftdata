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

import static io.ballerina.lib.aws.redshiftdata.NativeClientAdaptor.NATIVE_CLIENT;

/**
 * Represents the utility functions for processing query results.
 */
public class QueryResultProcessor {
    private static final String RESULT_ITERATOR_OBJECT = "ResultIterator";
    private static final String RESULT_ITERATOR_RESULT_RESPONSE = "ResultResponse";
    private static final String RESULT_ITERATOR_RECORD_TYPE = "RecordType";
    private static final String RESULT_ITERATOR_CURRENT_RESULT_INDEX = "Index";
    private static final String RESULT_ITERATOR_COLUMN_INDEX_MAP = "IndexMap"; // field name -> result column index
    private static final String RESULT_ITERATOR_NATIVE_CLIENT = "nativeClient";
    private static final String RESULT_ITERATOR_STATEMENT_ID = "statementId";

    private QueryResultProcessor() {
    }

    public static BStream getRecordStream(RedshiftDataClient nativeClient, String statementId,
                                          GetStatementResultResponse nativeResultResponse, BTypedesc recordType)
            throws Exception {
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
                    throw new Exception("Field '" + ballerinaField + "' not found in the result set.");
                }
                columnIndexMap.put(ballerinaField, columnIndex);
            }
            boolean isClosedRecord = streamConstraint.isSealed();
            if (isClosedRecord) {
                // Ensure no extra fields are present in result set
                for (String resultField : resultFields) {
                    if (!columnIndexMap.containsKey(resultField)) {
                        throw new Exception("Field '" + resultField + "' not found in the record type.");
                    }
                }
            } else {
                // Add all the fields from the result set to the record type
                for (int i = 0; i < resultFields.size(); i++) {
                    if (!columnIndexMap.containsKey(resultFields.get(i))) {
                        columnIndexMap.put(resultFields.get(i), i);
                    }
                }
            }

            BObject resultIterator = ValueCreator.createObjectValue(ModuleUtils.getModule(), RESULT_ITERATOR_OBJECT);
            resultIterator.addNativeData(RESULT_ITERATOR_RESULT_RESPONSE, nativeResultResponse);
            resultIterator.addNativeData(RESULT_ITERATOR_CURRENT_RESULT_INDEX, 0L);
            resultIterator.addNativeData(RESULT_ITERATOR_COLUMN_INDEX_MAP, columnIndexMap);
            resultIterator.addNativeData(RESULT_ITERATOR_RECORD_TYPE, streamConstraint);
            // Add additional data for fetching the next result set
            resultIterator.addNativeData(RESULT_ITERATOR_STATEMENT_ID, statementId);
            resultIterator.addNativeData(NATIVE_CLIENT, nativeClient);

            return ValueCreator.createStreamValue(TypeCreator.createStreamType(streamConstraint,
                    PredefinedTypes.TYPE_NULL), resultIterator);
        } catch (Exception e) {
            throw new Exception("Error occurred while creating the Record Stream: "
                    + Objects.requireNonNullElse(e.getMessage(), "Unknown error"));
        }
    }

    @SuppressWarnings("unchecked")
    public static Object nextResult(BObject bResultIterator) {
        RecordType recordType = (RecordType) bResultIterator.getNativeData(RESULT_ITERATOR_RECORD_TYPE);
        long index = (long) bResultIterator.getNativeData(RESULT_ITERATOR_CURRENT_RESULT_INDEX);
        Map<String, Integer> columnIndexMap = (Map<String, Integer>) bResultIterator
                .getNativeData(RESULT_ITERATOR_COLUMN_INDEX_MAP);
        GetStatementResultResponse resultResponse = (GetStatementResultResponse) bResultIterator
                .getNativeData(RESULT_ITERATOR_RESULT_RESPONSE);

        List<List<Field>> rows = resultResponse.records();
        try {
            // Fetch the next record when the current result set is processed
            if (index >= rows.size() && Objects.nonNull(resultResponse.nextToken())) {
                RedshiftDataClient nativeClient = (RedshiftDataClient) bResultIterator
                        .getNativeData(RESULT_ITERATOR_NATIVE_CLIENT);
                String statementId = (String) bResultIterator.getNativeData(RESULT_ITERATOR_STATEMENT_ID);

                resultResponse = nativeClient.getStatementResult(
                        GetStatementResultRequest.builder()
                                .id(statementId).nextToken(resultResponse.nextToken()).build());
                rows = resultResponse.records();
                index = 0;
                bResultIterator.addNativeData(RESULT_ITERATOR_CURRENT_RESULT_INDEX, index);
                bResultIterator.addNativeData(RESULT_ITERATOR_RESULT_RESPONSE, resultResponse);
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
                bResultIterator.addNativeData(RESULT_ITERATOR_CURRENT_RESULT_INDEX, index + 1);
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
        if (field.stringValue() != null) {
            return StringUtils.fromString(field.stringValue());
        }
        if (field.booleanValue() != null) {
            return field.booleanValue();
        }
        if (field.longValue() != null) {
            return field.longValue();
        }
        if (field.doubleValue() != null) {
            return field.doubleValue();
        }
        return null;
    }

    public static void closeResult(BObject recordIterator) {
        try {
            recordIterator.addNativeData(RESULT_ITERATOR_RESULT_RESPONSE, null);
            recordIterator.addNativeData(RESULT_ITERATOR_RECORD_TYPE, null);
            recordIterator.addNativeData(RESULT_ITERATOR_CURRENT_RESULT_INDEX, null);
            recordIterator.addNativeData(RESULT_ITERATOR_COLUMN_INDEX_MAP, null);
            recordIterator.addNativeData(RESULT_ITERATOR_NATIVE_CLIENT, null);
            recordIterator.addNativeData(RESULT_ITERATOR_STATEMENT_ID, null);
        } catch (Exception e) {
            throw new RuntimeException("Error occurred while closing the Query result: " + e.getMessage());
        }
    }
}
