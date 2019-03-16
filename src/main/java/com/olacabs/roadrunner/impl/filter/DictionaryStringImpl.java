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
package com.olacabs.roadrunner.impl.filter;

import java.util.Arrays;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;

import com.olacabs.roadrunner.api.filter.DictionaryString;

public class DictionaryStringImpl implements DictionaryString {

	protected Map<String, String> data = new ConcurrentHashMap<>();
	String[] keyArray = null;
	String[] keyArrayL = null;

	public DictionaryStringImpl() {
		this.data = new ConcurrentHashMap<String, String>();
	}

	@Override
	public boolean add(String k) {
		if(! data.containsKey(k)) {
			data.put(k, k.toLowerCase());
		}
		return true;
	}

	@Override
	public String[] keys() {
		if ( null == this.keyArray) {
			fillToArray();
			return this.keyArray;
		} else {
			int size = this.data.size();
			if ( this.keyArray.length == size) return this.keyArray;
			else {
				fillToArray();
				return this.keyArray;
			}
		}
	}
	
	@Override
	public String[] lowercaseKeys() {
		if ( null == this.keyArrayL) {
			fillToArray();
			return this.keyArrayL;
		} else {
			int size = this.data.size();
			if ( this.keyArrayL.length == size) return this.keyArrayL;
			else {
				fillToArray();
				return this.keyArrayL;
			}
		}
	}

	private synchronized final void fillToArray() {
		int i=0;
		int size = this.data.size();
		String[] tmpKey = new String[size];
		String[] tmpKeyL = new String[size];
		
		for(Entry<String, String> entry : this.data.entrySet()) {
			if(i >= size) break;
			
			tmpKey[i] = entry.getKey();
			tmpKeyL[i] = entry.getValue();
			i += 1;
		}
		this.keyArray = tmpKey;
		this.keyArrayL = tmpKeyL;
	}

	public static void main(String[] args) {
		String[] str = new String[]{"A", "a", "b", "ba", "bg", "f", "text", "z"};
		System.out.println(Arrays.binarySearch(str, "A"));
	}

}
