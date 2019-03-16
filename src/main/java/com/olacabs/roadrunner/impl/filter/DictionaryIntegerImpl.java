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

package com.olacabs.roadrunner.impl.filter;

import java.util.Arrays;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import com.olacabs.roadrunner.api.filter.DictionaryInteger;

/**
 * Don't Edit this code, it is generated using templates
 */
public class DictionaryIntegerImpl implements DictionaryInteger {
	protected Map<Integer, Object> data = new ConcurrentHashMap<>();
	int[] keyArray = null;

	private static final Object PRESENT = new Object();

	public DictionaryIntegerImpl() {
		this.data = new ConcurrentHashMap<Integer, Object>();
	}
	
	@Override
	public final boolean add(int k) {
		if(! data.containsKey(k)) {
			data.put(k, PRESENT);
		}
		return true;
	}

	@Override
	public final int[] keys() {
		if ( null == this.keyArray) {
			return fillToArray();

		} else {
			int size = this.data.size();
			if ( this.keyArray.length == size) return this.keyArray;
			else {
				return fillToArray();
			}
		}
	}

	private synchronized final int[] fillToArray() {
		int i=0;
		int size = this.data.size();
		int[] tmpKeyA = new int[size];
		Iterator<Integer> keyI = this.data.keySet().iterator();
		while ( i < size && keyI.hasNext() ) {
			tmpKeyA[i++] = keyI.next();
		}
		Arrays.sort(tmpKeyA);
		this.keyArray = tmpKeyA;
		return this.keyArray;
	}

	@Override
	public final int[] rangeValues(int startMatch, int endMatch) {
		int[] localCopy = keys();
		int length = localCopy.length;
	
		int gtePos = Arrays.binarySearch(localCopy, startMatch);
		if (gtePos < 0) gtePos = (gtePos * -1) - 1;
       
        if (gtePos < length) {
            int ltePos = Arrays.binarySearch(localCopy, gtePos, length, endMatch);
            if (ltePos < 0) ltePos = (ltePos * -1) - 2;
          
            if(ltePos >= 0) {
            	int[] newArr = new int[(ltePos - gtePos) + 1];
            	int count = 0;
            	
            	for(int i = gtePos; i <= ltePos; i++) {
            		newArr[count++] = keyArray[i]; 
            	}
            	return newArr;
            }
        } 
		return new int[]{}; 
	}

	@Override
	public final int[] greaterThan(int value) {
		int[] localCopy = keys();
		int length = localCopy.length;
		
		int pos = Arrays.binarySearch(localCopy, value);
        if (pos >= 0) pos++;
        else pos = (pos * -1) - 1;
        
        if (pos < length) {
        	int[] newArr = new int[length - pos];
        	int count = 0;
        	
        	for(int i = pos; i < length; i++) {
        		newArr[count++] = keyArray[i]; 
        	}
        	return newArr;
        }
        return new int[]{};
	}

	@Override
	public final int[] greaterThanEqualTo(int value) {
		int[] localCopy = keys();
		int length = localCopy.length;
		
		int pos = Arrays.binarySearch(localCopy, value);
		if (pos < 0) pos = (pos * -1) - 1;
        
        if (pos < length) {
        	int[] newArr = new int[length - pos];
        	int count = 0;
        	
        	for(int i = pos; i < length; i++) {
        		newArr[count++] = keyArray[i]; 
        	}
        	return newArr;
        }
        return new int[]{};
	}

	@Override
	public final int[] lessThan(int value) {
		int[] localCopy = keys();
		int length = localCopy.length;
		
		int pos = Arrays.binarySearch(localCopy, value);
		if (pos > 0) pos--;
	    else pos = (pos * -1) - 2;
        
        if (pos >= 0) {
        	int[] newArr = new int[pos + 1];
        	int count = 0;
        	
        	for(int i = 0; i <= pos; i++) {
        		newArr[count++] = keyArray[i]; 
        	}
        	return newArr;
        }
        return new int[]{};
	}

	@Override
	public final int[] lessThanEqualTo(int value) {
		int[] localCopy = keys();
		int length = localCopy.length;
		
		int pos = Arrays.binarySearch(localCopy, value);
		if (pos < 0) pos = (pos * -1) - 2;
        
        if (pos >= 0) {
        	int[] newArr = new int[pos + 1];
        	int count = 0;
        	
        	for(int i = 0; i <= pos; i++) {
        		newArr[count++] = keyArray[i]; 
        	}
        	return newArr;
        }
        return new int[]{};
	}
	
	@Override
	public final boolean contains(int value) {
		int[] localCopy = keys();
		int pos = Arrays.binarySearch(localCopy, value);
		if (pos >= 0) {
			return true;
		}

		return false;
	}
}

