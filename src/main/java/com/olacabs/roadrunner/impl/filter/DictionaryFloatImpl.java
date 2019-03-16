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

import com.olacabs.roadrunner.api.filter.DictionaryFloat;

/**
 * Don't Edit this code, it is generated using templates
 */
public class DictionaryFloatImpl implements DictionaryFloat {
	protected Map<Float, Object> data = new ConcurrentHashMap<>();
	float[] keyArray = null;

	private static final Object PRESENT = new Object();

	public DictionaryFloatImpl() {
		this.data = new ConcurrentHashMap<Float, Object>();
	}
	
	@Override
	public final boolean add(float k) {
		if(! data.containsKey(k)) {
			data.put(k, PRESENT);
		}
		return true;
	}

	@Override
	public final float[] keys() {
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

	private synchronized final float[] fillToArray() {
		int i=0;
		int size = this.data.size();
		float[] tmpKeyA = new float[size];
		Iterator<Float> keyI = this.data.keySet().iterator();
		while ( i < size && keyI.hasNext() ) {
			tmpKeyA[i++] = keyI.next();
		}
		Arrays.sort(tmpKeyA);
		this.keyArray = tmpKeyA;
		return this.keyArray;
	}

	@Override
	public final float[] rangeValues(float startMatch, float endMatch) {
		float[] localCopy = keys();
		int length = localCopy.length;
	
		int gtePos = Arrays.binarySearch(localCopy, startMatch);
		if (gtePos < 0) gtePos = (gtePos * -1) - 1;
       
        if (gtePos < length) {
            int ltePos = Arrays.binarySearch(localCopy, gtePos, length, endMatch);
            if (ltePos < 0) ltePos = (ltePos * -1) - 2;
          
            if(ltePos >= 0) {
            	float[] newArr = new float[(ltePos - gtePos) + 1];
            	int count = 0;
            	
            	for(int i = gtePos; i <= ltePos; i++) {
            		newArr[count++] = keyArray[i]; 
            	}
            	return newArr;
            }
        } 
		return new float[]{}; 
	}

	@Override
	public final float[] greaterThan(float value) {
		float[] localCopy = keys();
		int length = localCopy.length;
		
		int pos = Arrays.binarySearch(localCopy, value);
        if (pos >= 0) pos++;
        else pos = (pos * -1) - 1;
        
        if (pos < length) {
        	float[] newArr = new float[length - pos];
        	int count = 0;
        	
        	for(int i = pos; i < length; i++) {
        		newArr[count++] = keyArray[i]; 
        	}
        	return newArr;
        }
        return new float[]{};
	}

	@Override
	public final float[] greaterThanEqualTo(float value) {
		float[] localCopy = keys();
		int length = localCopy.length;
		
		int pos = Arrays.binarySearch(localCopy, value);
		if (pos < 0) pos = (pos * -1) - 1;
        
        if (pos < length) {
        	float[] newArr = new float[length - pos];
        	int count = 0;
        	
        	for(int i = pos; i < length; i++) {
        		newArr[count++] = keyArray[i]; 
        	}
        	return newArr;
        }
        return new float[]{};
	}

	@Override
	public final float[] lessThan(float value) {
		float[] localCopy = keys();
		int length = localCopy.length;
		
		int pos = Arrays.binarySearch(localCopy, value);
		if (pos > 0) pos--;
	    else pos = (pos * -1) - 2;
        
        if (pos >= 0) {
        	float[] newArr = new float[pos + 1];
        	int count = 0;
        	
        	for(int i = 0; i <= pos; i++) {
        		newArr[count++] = keyArray[i]; 
        	}
        	return newArr;
        }
        return new float[]{};
	}

	@Override
	public final float[] lessThanEqualTo(float value) {
		float[] localCopy = keys();
		int length = localCopy.length;
		
		int pos = Arrays.binarySearch(localCopy, value);
		if (pos < 0) pos = (pos * -1) - 2;
        
        if (pos >= 0) {
        	float[] newArr = new float[pos + 1];
        	int count = 0;
        	
        	for(int i = 0; i <= pos; i++) {
        		newArr[count++] = keyArray[i]; 
        	}
        	return newArr;
        }
        return new float[]{};
	}
	
	@Override
	public final boolean contains(float value) {
		float[] localCopy = keys();
		int pos = Arrays.binarySearch(localCopy, value);
		if (pos >= 0) {
			return true;
		}

		return false;
	}
}

