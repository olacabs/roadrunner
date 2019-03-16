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

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import com.olacabs.roadrunner.api.IndexableField;
import com.olacabs.roadrunner.api.filter.DictionaryDouble;
import com.olacabs.roadrunner.api.filter.DictionaryFloat;
import com.olacabs.roadrunner.api.filter.DictionaryInteger;
import com.olacabs.roadrunner.api.filter.DictionaryLong;
import com.olacabs.roadrunner.api.filter.DictionaryShort;
import com.olacabs.roadrunner.api.filter.DictionaryString;
import com.olacabs.roadrunner.impl.filter.DictionaryDoubleImpl;
import com.olacabs.roadrunner.impl.filter.DictionaryIntegerImpl;
import com.olacabs.roadrunner.impl.filter.DictionaryLongImpl;
import com.olacabs.roadrunner.impl.filter.DictionaryShortImpl;
import com.olacabs.roadrunner.impl.filter.DictionaryFloatImpl;
import com.olacabs.roadrunner.impl.filter.DictionaryStringImpl;

public class DictionaryHolder {
	
	Map<String, DictionaryShort> shortDictionary = new ConcurrentHashMap<>();
	
	Map<String, DictionaryInteger> intDictionary = new ConcurrentHashMap<>();
	
	Map<String, DictionaryFloat> floatDictionary = new ConcurrentHashMap<>();
	
	Map<String, DictionaryDouble> doubleDictionary = new ConcurrentHashMap<>();
	
	Map<String, DictionaryLong> longDictionary = new ConcurrentHashMap<>();
	
	Map<String, DictionaryString> stringDictionary = new ConcurrentHashMap<>();
	
	public void createDictionary(IndexableField<?> field) {
		switch (field.getDataType()) {
		case SHORT: case ARRSHORT:
			shortDictionary.put(field.getName(), new DictionaryShortImpl());
			break;
		
		case INT: case ARRINT:
			intDictionary.put(field.getName(), new DictionaryIntegerImpl());
			break;
		
		case FLOAT: case ARRFLOAT:
			floatDictionary.put(field.getName(), new DictionaryFloatImpl());
			break;
			
		case DOUBLE: case ARRDOUBLE:
			doubleDictionary.put(field.getName(), new DictionaryDoubleImpl());
			break;
		
		case LONG: case ARRLONG:
			longDictionary.put(field.getName(), new DictionaryLongImpl());
			break;
		
		case STRING: case ARRSTRING:
			stringDictionary.put(field.getName(), new DictionaryStringImpl());
			break;
			
		default:
			break;
		}
	}
	
	public void addShort(String key, short... values) {
		DictionaryShortImpl impl = (DictionaryShortImpl) shortDictionary.get(key);
		if(impl == null) {
			throw new RuntimeException("Should have been initialized");
		}
		for(short value : values) {
			impl.add(value);
		}
	}
	
	public void addInteger(String key, int... values) {
		DictionaryIntegerImpl impl = (DictionaryIntegerImpl) intDictionary.get(key);
		if(impl == null) {
			throw new RuntimeException("Should have been initialized");
		}
		for(int value : values) {
			impl.add(value);
		}
	}
	
	public void addFloat(String key, float... values) {
		DictionaryFloatImpl impl = (DictionaryFloatImpl) floatDictionary.get(key);
		if(impl == null) {
			throw new RuntimeException("Should have been initialized");
		}
		for(float value : values) {
			impl.add(value);
		}
	}
	
	public void addDouble(String key, double... values) {
		DictionaryDoubleImpl impl = (DictionaryDoubleImpl) doubleDictionary.get(key);
		if(impl == null) {
			throw new RuntimeException("Should have been initialized");
		}
		for(double value : values) {
			impl.add(value);
		}
	}
	
	public void addLong(String key, long... values) {
		DictionaryLongImpl impl = (DictionaryLongImpl) longDictionary.get(key);
		if(impl == null) {
			throw new RuntimeException("Should have been initialized");
		}
		for(long value : values) {
			impl.add(value);
		}
	}
	
	public void addString(String key, String... values) {
		DictionaryStringImpl impl = (DictionaryStringImpl) stringDictionary.get(key);
		if(impl == null) {
			throw new RuntimeException("Should have been initialized");
		}
		for(String value : values) {
			impl.add(value);
		}
	}

}
