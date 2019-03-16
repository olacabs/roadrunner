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

import com.olacabs.roadrunner.api.filter.DictionaryLong;

/**
 * Don't Edit this code, it is generated using templates
 */
public class DictionaryLongImpl implements DictionaryLong {
	protected Map<Long, Object> data = new ConcurrentHashMap<>();
	long[] keyArray = null;

	private static final Object PRESENT = new Object();

	public DictionaryLongImpl() {
		this.data = new ConcurrentHashMap<Long, Object>();
	}
	
	@Override
	public final boolean add(long k) {
		if(! data.containsKey(k)) {
			data.put(k, PRESENT);
		}
		return true;
	}

	@Override
	public final long[] keys() {
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

	private synchronized final long[] fillToArray() {
		int i=0;
		int size = this.data.size();
		long[] tmpKeyA = new long[size];
		Iterator<Long> keyI = this.data.keySet().iterator();
		while ( i < size && keyI.hasNext() ) {
			tmpKeyA[i++] = keyI.next();
		}
		Arrays.sort(tmpKeyA);
		this.keyArray = tmpKeyA;
		return this.keyArray;
	}

	@Override
	public final long[] rangeValues(long startMatch, long endMatch) {
		long[] localCopy = keys();
		int length = localCopy.length;
	
		int gtePos = Arrays.binarySearch(localCopy, startMatch);
		if (gtePos < 0) gtePos = (gtePos * -1) - 1;
       
        if (gtePos < length) {
            int ltePos = Arrays.binarySearch(localCopy, gtePos, length, endMatch);
            if (ltePos < 0) ltePos = (ltePos * -1) - 2;
          
            if(ltePos >= 0) {
            	long[] newArr = new long[(ltePos - gtePos) + 1];
            	int count = 0;
            	
            	for(int i = gtePos; i <= ltePos; i++) {
            		newArr[count++] = keyArray[i]; 
            	}
            	return newArr;
            }
        } 
		return new long[]{}; 
	}

	@Override
	public final long[] greaterThan(long value) {
		long[] localCopy = keys();
		int length = localCopy.length;
		
		int pos = Arrays.binarySearch(localCopy, value);
        if (pos >= 0) pos++;
        else pos = (pos * -1) - 1;
        
        if (pos < length) {
        	long[] newArr = new long[length - pos];
        	int count = 0;
        	
        	for(int i = pos; i < length; i++) {
        		newArr[count++] = keyArray[i]; 
        	}
        	return newArr;
        }
        return new long[]{};
	}

	@Override
	public final long[] greaterThanEqualTo(long value) {
		long[] localCopy = keys();
		int length = localCopy.length;
		
		int pos = Arrays.binarySearch(localCopy, value);
		if (pos < 0) pos = (pos * -1) - 1;
        
        if (pos < length) {
        	long[] newArr = new long[length - pos];
        	int count = 0;
        	
        	for(int i = pos; i < length; i++) {
        		newArr[count++] = keyArray[i]; 
        	}
        	return newArr;
        }
        return new long[]{};
	}

	@Override
	public final long[] lessThan(long value) {
		long[] localCopy = keys();
		int length = localCopy.length;
		
		int pos = Arrays.binarySearch(localCopy, value);
		if (pos > 0) pos--;
	    else pos = (pos * -1) - 2;
        
        if (pos >= 0) {
        	long[] newArr = new long[pos + 1];
        	int count = 0;
        	
        	for(int i = 0; i <= pos; i++) {
        		newArr[count++] = keyArray[i]; 
        	}
        	return newArr;
        }
        return new long[]{};
	}

	@Override
	public final long[] lessThanEqualTo(long value) {
		long[] localCopy = keys();
		int length = localCopy.length;
		
		int pos = Arrays.binarySearch(localCopy, value);
		if (pos < 0) pos = (pos * -1) - 2;
        
        if (pos >= 0) {
        	long[] newArr = new long[pos + 1];
        	int count = 0;
        	
        	for(int i = 0; i <= pos; i++) {
        		newArr[count++] = keyArray[i]; 
        	}
        	return newArr;
        }
        return new long[]{};
	}
	
	@Override
	public final boolean contains(long value) {
		long[] localCopy = keys();
		int pos = Arrays.binarySearch(localCopy, value);
		if (pos >= 0) {
			return true;
		}

		return false;
	}
}

