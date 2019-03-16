//************************************************************
// Copyright 2019 ANI Technologies Pvt. Ltd.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//************************************************************
package com.olacabs.roadrunner.api;

import java.util.Comparator;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListMap;

public interface Record extends Map<String, Object> {

    Record create();

    Record setSpecialFieldNames(String latFieldName, String lonFieldName);

    Record setIDFieldName(String idField);
    Record setLatField(double lat);
    Record setLonField(double lon);

    Record setId(String recordId);
    String getId();
    String getPartitionValue();
    double getLat();
    double getLon();

    //TODO:Remove getLatCosine
    //double getLatCosine();
    double measureDistanceInMeters( double latitude, double longitude, double latitudeCosine);

    Record setPartitionValue(String partitionValue);
    Record setField(String fldName, Object val);

    Object getField(String fldName);
    Object getTokenizedField(String fldName);
    Object getTokenizedField(final String[] fldNameTokens);
    Boolean getBooleanField(String fldName);
    Byte getByteField(String fldName);
    Short getShortField(String fldName);
    Integer getIntegerField(String fldName);
    Float getFloatField(String fldName);
    Long getLongField(String fldName);
    Double getDoubleField(String fldName);
    String getStringField(String fldName);
  
    short[] getShortArrayField(String fldName);
    int[] getIntegerArrayField(String fldName);
    float[] getFloatArrayField(String fldName);
    long[] getLongArrayField(String fldName);
    double[] getDoubleArrayField(String fldName);
    String[] getStringArrayField(String fldName);

    void merge(Map<? extends String, ? extends Object> m);

    static final Class hashMapClazz = HashMap.class;
    static final Class linkedHashMapClazz = LinkedHashMap.class;
    static final Class concurrentHashMapClazz = ConcurrentHashMap.class;
    static final Class treeMapClazz = TreeMap.class;
    static final Class enumMapClazz = EnumMap.class;
    static final Class hashtableClazz = Hashtable.class;
    static final Class concurrentSkipListClazz = ConcurrentSkipListMap.class;
    static final String PARENT_PATH = "";

    default Record cloneSelectAll() {
        return cloneSelectAll(true);
    }

    default Record cloneSelectAll(final boolean deepCopyNestedHashMap) {
        Record r = create();
        if(deepCopyNestedHashMap) {
            r.putAll(cloneAll(this, PARENT_PATH, true, null, null));
        } else {
            r.putAll(this);
        }
        return r;
    }

    default Record cloneSelectAll(final Set<String> deepCopyFields, final HashMap<String, Object> outputMap) {
        if(deepCopyFields == null || deepCopyFields.isEmpty()) {
            throw new IllegalArgumentException("Use cloneSelectAll(false) method");
        }

        Record r = create();
        if(deepCopyFields != null) {
            r.putAll(cloneAll(this, PARENT_PATH, false, deepCopyFields, outputMap));
        } else {
            r.putAll(this);
        }
        return r;
    }

    default Map<String, Object> cloneAll(final Map<String, Object> inputMap, final String path,
                                               final boolean deepCopyNestedHashMap, final Set<String> deepCopyFields, Map<String, Object> outputMap ) {

        if ( null == outputMap) outputMap = new HashMap<String, Object>(inputMap.size() * 4 / 3 + 1); //Resize restricted

        String newPath = PARENT_PATH;

        for (Entry<String, Object> entry : inputMap.entrySet()) {
            String key = entry.getKey();
            Object value = entry.getValue();

            boolean process = false;
            if(value != null ) {
                if ( deepCopyNestedHashMap ) {
                    if ( checkIfInstanceOfMap(value)) {
                        process = true;
                    }
                } else if ( deepCopyFields != null ) {
                    if ( checkIfInstanceOfMap(value) ) {
                        newPath = (path == PARENT_PATH) ? key : path + "." + key; //No object creation if not a hashmap
                        if ( deepCopyFields.contains(newPath) ) { //Only hashmaps undergo check from Set
                            process = true;
                        }
                    }
                }
            }

            if ( process ) {
                outputMap.put(key, cloneAll((Map<String, Object>) value, newPath, deepCopyNestedHashMap, deepCopyFields, null));
            } else {
                outputMap.put(key, value);
            }
        }
        return outputMap;

    }

    default Map<String, Object> cloneAll(final Map<String, Object> inputMap) {
        Map<String, Object> outputMap = new HashMap<String, Object>(inputMap.size() * 4 / 3 + 1);
        for (Entry<String, Object> entry : inputMap.entrySet()) {
            String key = entry.getKey();
            Object value = entry.getValue();
            if(value != null && checkIfInstanceOfMap(value)) {
                outputMap.put(key, cloneAll((Map<String, Object>) value));
            } else {
                outputMap.put(key, value);
            }
        }
        return outputMap;
    }


    default Record cloneSelectInclusive(final String[] fields) {
        return cloneSelectInclusive(fields, true);
    }

    default Record cloneSelectInclusive(final String[] fields, final boolean deepCopyNestedHashMap) {
        Record r = create();
        for(String field : fields) {
            String[] tokens = field.split("\\.");
            int length = tokens.length;
            if(length == 1) {
                if(this.containsKey(field)) {
                    Object value = this.get(field);
                    if(deepCopyNestedHashMap && value != null && checkIfInstanceOfMap(value)) {
                        r.put(field, cloneAll((Map<String, Object>) value));

                    } else {
                        r.put(field, value);
                    }
                }
                continue;
            }
            Map<String, Object> intermediateDataMap = this;
            Map<String, Object> intermediateRecordMap = r;
            for(int i = 0; i < length - 1; i++) {
                Object mapObj = intermediateDataMap.get(tokens[i]);
                if(! (checkIfInstanceOfMap(mapObj))) {
                    removeParentMapsIfEmpty(r, tokens, i);
                    break;
                }
                intermediateDataMap = (Map<String, Object>) mapObj;
                if(intermediateDataMap == null) {
                    removeParentMapsIfEmpty(r, tokens, i);
                    break;
                };
                if(intermediateRecordMap.get(tokens[i]) == null) {
                    Map<String, Object> innerMap = new HashMap<>();
                    intermediateRecordMap.put(tokens[i], innerMap);
                    intermediateRecordMap = innerMap;
                } else {
                    intermediateRecordMap = (Map<String, Object>) intermediateRecordMap.get(tokens[i]);
                }
            }
            if(intermediateDataMap != null) {
                String key = tokens[length - 1];
                if(intermediateDataMap.containsKey(key)) {
                    Object value = intermediateDataMap.get(key);
                    if(deepCopyNestedHashMap && value != null && checkIfInstanceOfMap(value)) {
                        intermediateRecordMap.put(key, cloneAll((Map<String, Object>) value));

                    } else {
                        intermediateRecordMap.put(key, value);
                    }
                } else {
                    removeParentMapsIfEmpty(r, tokens, length - 1);
                }
            }
        }
        return r;
    }


    default boolean checkIfOnlyChild(final Record r, final String[] tokens, final int startIndex, final int endIndex) {
        Map<String, Object> intermediateRecordMap = r;
        boolean onlyChild = true;
        for(int i = 0; i <= endIndex; i++) {
            intermediateRecordMap = (Map<String, Object>) intermediateRecordMap.get(tokens[i]);
            if(i >= startIndex) {
                if(intermediateRecordMap == null || intermediateRecordMap.isEmpty()) {
                    break;
                }
                if(intermediateRecordMap.size() > 1 || (intermediateRecordMap.size() == 1 && ! intermediateRecordMap.containsKey(tokens[i + 1]))) {
                    onlyChild = false;
                    break;
                }
            }
        }
        return onlyChild;
    }

    default void removeParentMapsIfEmpty(final Record r, final String[] tokens, final int length) {
        Map<String, Object> intermediateRecordMap = r;
        for(int i = 0; i < length; i++) {
            boolean checkIfOnlyChild = checkIfOnlyChild(r, tokens, i, length - 1);
            if(checkIfOnlyChild) {
                intermediateRecordMap.remove(tokens[i]);
                break;
            }
            intermediateRecordMap = (Map<String, Object>) intermediateRecordMap.get(tokens[i]);
        }
    }


    default Record cloneSelectExclusive(final String[] fields) {
        Record r = (Record) cloneSelectAll(true);

        for(String field : fields) {
            String[] tokens = field.split("\\.");
            int length = tokens.length;
            if(length == 1) {
                r.remove(field);
                continue;
            }

            Map<String, Object> intermediateRecordMap = r;
            for(int i = 0; i < length - 1; i++) {
                Object mapObj = intermediateRecordMap.get(tokens[i]);
                if(mapObj == null || ! (checkIfInstanceOfMap(mapObj))) break;

                intermediateRecordMap = (Map<String, Object>) mapObj;
            }
            if(intermediateRecordMap != null) {
                intermediateRecordMap.remove(tokens[length - 1]);
                removeParentMapsIfEmpty(r, tokens, length - 1);
            }
        }

        return r;
    }

    default boolean checkIfInstanceOfMap(final Object value) {
        if(value == null) return false;
        Class valClazz = value.getClass();
        return (valClazz.equals(hashMapClazz) || valClazz.equals(linkedHashMapClazz) || valClazz.equals(concurrentHashMapClazz)
                || valClazz.equals(treeMapClazz) || valClazz.equals(enumMapClazz) || valClazz.equals(hashtableClazz)
                || valClazz.equals(concurrentSkipListClazz));
    }

    static <T extends Comparable<T>> Comparator<Record> buildComparator(final String fldName) {
        Comparator<Record> comparator = new Comparator<Record>() {
            @Override
            public int compare(Record o1, Record o2) {
                T val1 = (T) o1.getTokenizedField(fldName);
                T val2 = (T) o2.getTokenizedField(fldName);
                if(val1 == null) {
                    return -1;
                } else if(val2 == null) {
                    return 1;
                } else {
                    return val1.compareTo(val2);
                }
            }
        };
        return comparator;
    }
}
