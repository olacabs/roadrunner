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
package com.olacabs.roadrunner.utils;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

/*
 * Simple class that extends the regular java.util.HashMap but overrides the
 * toString() method of the HashMap class to produce a JSON string instead
 *
 * https://github.com/apache/arrow/blob/f7ab7270bb07466dabf84c015a6db2a192eb3dad/java/vector/src/main/java/org/apache/arrow/vector/file/json/JsonFileReader.java
 */
public class JsonStringHashMap<K, V> extends LinkedHashMap<K, V> {

    private static ObjectMapper mapper;

    static {
        mapper = new ObjectMapper();
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null) {
            return false;
        }
        if (!(obj instanceof Map)) {
            return false;
        }
        Map<?, ?> other = (Map<?, ?>) obj;
        if (this.size() != other.size()) {
            return false;
        }
        for (K key : this.keySet()) {
            if (this.get(key) == null ) {
                if (other.get(key) == null) {
                    continue;
                } else {
                    return false;
                }
            }
            if ( ! this.get(key).equals(other.get(key))) {
                return false;
            }
        }
        return true;
    }

    @Override
    public final String toString() {
        try {
            return mapper.writeValueAsString(this);
        } catch(JsonProcessingException e) {
            throw new IllegalStateException("Cannot serialize hash map to JSON string", e);
        }
    }

}