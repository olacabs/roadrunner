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

import com.olacabs.roadrunner.api.filter.DictionaryShort;

/**
 * Don't Edit this code, it is generated using templates
 */
public class DictionaryShortImpl implements DictionaryShort {
	protected Map<Short, Object> data = new ConcurrentHashMap<>();
	short[] keyArray = null;

	private static final Object PRESENT = new Object();

	public DictionaryShortImpl() {
		this.data = new ConcurrentHashMap<Short, Object>();
	}
	
	@Override
	public final boolean add(short k) {
		if(! data.containsKey(k)) {
			data.put(k, PRESENT);
		}
		return true;
	}

	@Override
	public final short[] keys() {
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

	private synchronized final short[] fillToArray() {
		int i=0;
		int size = this.data.size();
		short[] tmpKeyA = new short[size];
		Iterator<Short> keyI = this.data.keySet().iterator();
		while ( i < size && keyI.hasNext() ) {
			tmpKeyA[i++] = keyI.next();
		}
		Arrays.sort(tmpKeyA);
		this.keyArray = tmpKeyA;
		return this.keyArray;
	}

	@Override
	public final short[] rangeValues(short startMatch, short endMatch) {
		short[] localCopy = keys();
		int length = localCopy.length;
	
		int gtePos = Arrays.binarySearch(localCopy, startMatch);
		if (gtePos < 0) gtePos = (gtePos * -1) - 1;
       
        if (gtePos < length) {
            int ltePos = Arrays.binarySearch(localCopy, gtePos, length, endMatch);
            if (ltePos < 0) ltePos = (ltePos * -1) - 2;
          
            if(ltePos >= 0) {
            	short[] newArr = new short[(ltePos - gtePos) + 1];
            	int count = 0;
            	
            	for(int i = gtePos; i <= ltePos; i++) {
            		newArr[count++] = keyArray[i]; 
            	}
            	return newArr;
            }
        } 
		return new short[]{}; 
	}

	@Override
	public final short[] greaterThan(short value) {
		short[] localCopy = keys();
		int length = localCopy.length;
		
		int pos = Arrays.binarySearch(localCopy, value);
        if (pos >= 0) pos++;
        else pos = (pos * -1) - 1;
        
        if (pos < length) {
        	short[] newArr = new short[length - pos];
        	int count = 0;
        	
        	for(int i = pos; i < length; i++) {
        		newArr[count++] = keyArray[i]; 
        	}
        	return newArr;
        }
        return new short[]{};
	}

	@Override
	public final short[] greaterThanEqualTo(short value) {
		short[] localCopy = keys();
		int length = localCopy.length;
		
		int pos = Arrays.binarySearch(localCopy, value);
		if (pos < 0) pos = (pos * -1) - 1;
        
        if (pos < length) {
        	short[] newArr = new short[length - pos];
        	int count = 0;
        	
        	for(int i = pos; i < length; i++) {
        		newArr[count++] = keyArray[i]; 
        	}
        	return newArr;
        }
        return new short[]{};
	}

	@Override
	public final short[] lessThan(short value) {
		short[] localCopy = keys();
		int length = localCopy.length;
		
		int pos = Arrays.binarySearch(localCopy, value);
		if (pos > 0) pos--;
	    else pos = (pos * -1) - 2;
        
        if (pos >= 0) {
        	short[] newArr = new short[pos + 1];
        	int count = 0;
        	
        	for(int i = 0; i <= pos; i++) {
        		newArr[count++] = keyArray[i]; 
        	}
        	return newArr;
        }
        return new short[]{};
	}

	@Override
	public final short[] lessThanEqualTo(short value) {
		short[] localCopy = keys();
		int length = localCopy.length;
		
		int pos = Arrays.binarySearch(localCopy, value);
		if (pos < 0) pos = (pos * -1) - 2;
        
        if (pos >= 0) {
        	short[] newArr = new short[pos + 1];
        	int count = 0;
        	
        	for(int i = 0; i <= pos; i++) {
        		newArr[count++] = keyArray[i]; 
        	}
        	return newArr;
        }
        return new short[]{};
	}
	
	@Override
	public final boolean contains(short value) {
		short[] localCopy = keys();
		int pos = Arrays.binarySearch(localCopy, value);
		if (pos >= 0) {
			return true;
		}

		return false;
	}
}

