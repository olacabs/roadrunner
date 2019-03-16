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
/**
 * 
 */
package com.olacabs.roadrunner.impl;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.commons.lang3.StringUtils;

import com.olacabs.roadrunner.api.IndexableField;
import com.olacabs.roadrunner.api.Record;
import com.olacabs.roadrunner.api.IndexableField.FieldDataType;


public class RecordFlattener {
	
	private Map<String, IndexableField<?>> indexableFields;
	
	public RecordFlattener() {}

	public void setIndexableFields(Map<String, IndexableField<?>> indexableFields) {
		this.indexableFields = indexableFields;
	}

	private void doProcessElement(String propertyPrefix, Object element, Map<String, Object> resultMap) {
		if (element instanceof Map) {
			doFlatten(propertyPrefix, (Map<String, Object>) element, resultMap);
		} else {
			resultMap.put(propertyPrefix.intern(), element);
		}
	}
	
	public void transformArrayDataType(Map<String, Object> dataMap) {
		if(indexableFields == null) return;
		for(Entry<String, Object> entry : dataMap.entrySet()) {
			String key = entry.getKey();
			Object value = transformValue(key, entry.getValue());
			dataMap.put(key, value);
		}
	}

	private Object transformValue(String key, Object value) {
		IndexableField<?> field = indexableFields.get(key);
		if(field == null) return value;
		
		switch (field.getDataType()) {
		case ARRSHORT:
			{	
				if(value != null) {
					List<Short> dataList = (List<Short>) value;
					int size = dataList.size();
					short[] dataArray = new short[size];
		
					for(int i = 0; i < size; i++) {
						dataArray[i] = dataList.get(i);
					}
					value = dataArray;
				}
			}
			break;
		case ARRINT:
			{
				if(value != null) {
					List<Integer> dataList = (List<Integer>) value;
					int size = dataList.size();
					int[] dataArray = new int[size];
		
					for(int i = 0; i < size; i++) {
						dataArray[i] = dataList.get(i);
					}
					value = dataArray;
				}
			}
			break;
		case ARRFLOAT:
			{
				if(value != null) {
					List<Float> dataList = (List<Float>) value;
					int size = dataList.size();
					float[] dataArray = new float[size];
		
					for(int i = 0; i < size; i++) {
						dataArray[i] = dataList.get(i);
					}
					value = dataArray;
				}
			}
			break;
		case ARRLONG:
			{
				if(value != null) {
					List<Long> dataList = (List<Long>) value;
					int size = dataList.size();
					long[] dataArray = new long[size];
		
					for(int i = 0; i < size; i++) {
						dataArray[i] = dataList.get(i);
					}
					value = dataArray;
				}
			}
			break;
		case ARRDOUBLE:
			{
				if(value != null) {
					List<Double> dataList = (List<Double>) value;
					int size = dataList.size();
					double[] dataArray = new double[size];
		
					for(int i = 0; i < size; i++) {
						dataArray[i] = dataList.get(i);
					}
					value = dataArray;
				}
			}
			break;
		case ARRSTRING:
			{
				if(value != null) {
					List<String> dataList = (List<String>) value;
					int size = dataList.size();
					String[] dataArray = new String[size];
		
					for(int i = 0; i < size; i++) {
						dataArray[i] = dataList.get(i);
					}
					value = dataArray;
				}
			}
			break;
		default:
			break;
		}
		return value;
	}

	public void flattenMap(Map<String, Object> result, Record outputRecord) {
		doFlatten("", result, outputRecord);
	}

	private void doFlatten(String propertyPrefix, Map<String, Object> inputMap, Map<String, Object> resultMap) {
		if (StringUtils.isNotBlank(propertyPrefix)) {
			propertyPrefix = propertyPrefix + ".";
		}
		for (Entry<String, Object> entry : inputMap.entrySet()) {
			doProcessElement(propertyPrefix + entry.getKey(), entry.getValue(), resultMap);
		}
	}
}
