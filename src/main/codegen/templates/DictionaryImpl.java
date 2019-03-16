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
<@pp.dropOutputFile />
<#list roadRunnerTypes.numeric_types as type>

<#assign className="Dictionary${type.boxedType}Impl" />
<@pp.changeOutputFile name="/com/olacabs/roadrunner/impl/filter/${className}.java" />
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

import com.olacabs.roadrunner.api.filter.Dictionary${type.boxedType};

/**
 * Don't Edit this code, it is generated using templates
 */
public class ${className} implements Dictionary${type.boxedType} {
	protected Map<${type.boxedType}, Object> data = new ConcurrentHashMap<>();
	${type.javaType}[] keyArray = null;

	private static final Object PRESENT = new Object();

	public ${className}() {
		this.data = new ConcurrentHashMap<${type.boxedType}, Object>();
	}
	
	@Override
	public final boolean add(${type.javaType} k) {
		if(! data.containsKey(k)) {
			data.put(k, PRESENT);
		}
		return true;
	}

	@Override
	public final ${type.javaType}[] keys() {
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

	private synchronized final ${type.javaType}[] fillToArray() {
		int i=0;
		int size = this.data.size();
		${type.javaType}[] tmpKeyA = new ${type.javaType}[size];
		Iterator<${type.boxedType}> keyI = this.data.keySet().iterator();
		while ( i < size && keyI.hasNext() ) {
			tmpKeyA[i++] = keyI.next();
		}
		Arrays.sort(tmpKeyA);
		this.keyArray = tmpKeyA;
		return this.keyArray;
	}

	@Override
	public final ${type.javaType}[] rangeValues(${type.javaType} startMatch, ${type.javaType} endMatch) {
		${type.javaType}[] localCopy = keys();
		int length = localCopy.length;
	
		int gtePos = Arrays.binarySearch(localCopy, startMatch);
		if (gtePos < 0) gtePos = (gtePos * -1) - 1;
       
        if (gtePos < length) {
            int ltePos = Arrays.binarySearch(localCopy, gtePos, length, endMatch);
            if (ltePos < 0) ltePos = (ltePos * -1) - 2;
          
            if(ltePos >= 0) {
            	${type.javaType}[] newArr = new ${type.javaType}[(ltePos - gtePos) + 1];
            	int count = 0;
            	
            	for(int i = gtePos; i <= ltePos; i++) {
            		newArr[count++] = keyArray[i]; 
            	}
            	return newArr;
            }
        } 
		return new ${type.javaType}[]{}; 
	}

	@Override
	public final ${type.javaType}[] greaterThan(${type.javaType} value) {
		${type.javaType}[] localCopy = keys();
		int length = localCopy.length;
		
		int pos = Arrays.binarySearch(localCopy, value);
        if (pos >= 0) pos++;
        else pos = (pos * -1) - 1;
        
        if (pos < length) {
        	${type.javaType}[] newArr = new ${type.javaType}[length - pos];
        	int count = 0;
        	
        	for(int i = pos; i < length; i++) {
        		newArr[count++] = keyArray[i]; 
        	}
        	return newArr;
        }
        return new ${type.javaType}[]{};
	}

	@Override
	public final ${type.javaType}[] greaterThanEqualTo(${type.javaType} value) {
		${type.javaType}[] localCopy = keys();
		int length = localCopy.length;
		
		int pos = Arrays.binarySearch(localCopy, value);
		if (pos < 0) pos = (pos * -1) - 1;
        
        if (pos < length) {
        	${type.javaType}[] newArr = new ${type.javaType}[length - pos];
        	int count = 0;
        	
        	for(int i = pos; i < length; i++) {
        		newArr[count++] = keyArray[i]; 
        	}
        	return newArr;
        }
        return new ${type.javaType}[]{};
	}

	@Override
	public final ${type.javaType}[] lessThan(${type.javaType} value) {
		${type.javaType}[] localCopy = keys();
		int length = localCopy.length;
		
		int pos = Arrays.binarySearch(localCopy, value);
		if (pos > 0) pos--;
	    else pos = (pos * -1) - 2;
        
        if (pos >= 0) {
        	${type.javaType}[] newArr = new ${type.javaType}[pos + 1];
        	int count = 0;
        	
        	for(int i = 0; i <= pos; i++) {
        		newArr[count++] = keyArray[i]; 
        	}
        	return newArr;
        }
        return new ${type.javaType}[]{};
	}

	@Override
	public final ${type.javaType}[] lessThanEqualTo(${type.javaType} value) {
		${type.javaType}[] localCopy = keys();
		int length = localCopy.length;
		
		int pos = Arrays.binarySearch(localCopy, value);
		if (pos < 0) pos = (pos * -1) - 2;
        
        if (pos >= 0) {
        	${type.javaType}[] newArr = new ${type.javaType}[pos + 1];
        	int count = 0;
        	
        	for(int i = 0; i <= pos; i++) {
        		newArr[count++] = keyArray[i]; 
        	}
        	return newArr;
        }
        return new ${type.javaType}[]{};
	}
	
	@Override
	public final boolean contains(${type.javaType} value) {
		${type.javaType}[] localCopy = keys();
		int pos = Arrays.binarySearch(localCopy, value);
		if (pos >= 0) {
			return true;
		}

		return false;
	}
}
</#list>