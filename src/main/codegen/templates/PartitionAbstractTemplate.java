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

<#assign className="PartitionAbstract" />
<@pp.changeOutputFile name="/com/olacabs/roadrunner/impl/${className}.java" />
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

package com.olacabs.roadrunner.impl;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import com.olacabs.BitSetExposed;
import com.olacabs.roadrunner.api.IndexableField;

/**
 * Don't Edit this code, it is generated using templates
 */
public class ${className} {

	protected Map<String, BitSetExposed> nullStrips;
	
	<#list roadRunnerTypes.types as type>
	protected Map<String, ${type.javaType}[]> ${type.javaType}ArrayIndex;
	</#list>

	protected int capacity = 0;

	private static final short shortDefaultVal = -1;
	private static final int intDefaultVal = -1;
	private static final float floatDefaultVal = -1;
	private static final long longDefaultVal = -1;
	private static final double doubleDefaultVal = -1;
	private static final String StringDefaultVal = null;

	protected void expandMaps(int currentCapacity, int newCapacity) {
		
		<#list roadRunnerTypes.types as type>
//		Expanding Array index
		Map<String, ${type.javaType}[]> new${type.boxedType}ArrayIndex = new HashMap<String, ${type.javaType}[]>(${type.javaType}ArrayIndex);
        {
            for (Entry<String, ${type.javaType}[]> entry : new${type.boxedType}ArrayIndex.entrySet()) {
            	${type.javaType}[] ${type.javaType}Array = new ${type.javaType}[newCapacity];
                System.arraycopy(entry.getValue(), 0, ${type.javaType}Array, 0, currentCapacity);
                Arrays.fill(${type.javaType}Array, currentCapacity, newCapacity, ${type.javaType}DefaultVal);
                entry.setValue(${type.javaType}Array);
            }
            Map<String, ${type.javaType}[]> tmp = ${type.javaType}ArrayIndex;
            ${type.javaType}ArrayIndex = new${type.boxedType}ArrayIndex;
            tmp.clear();
        }
        </#list>

		capacity = newCapacity;

	}
	
	protected void updateArrayIndex(RecordImplVersion recordImplVersion, IndexableField<?> indexableField, int size) {
		int pos = recordImplVersion.getPOS();
		String fieldName = indexableField.getName();
		
		switch (indexableField.getDataType()) {
		<#list roadRunnerTypes.types as type>
		case ${type.caseCode}:
			{
				if(${type.javaType}ArrayIndex.get(fieldName) == null) {
					${type.javaType}ArrayIndex.put(fieldName, new ${type.javaType}[size]);
				} 
				${type.boxedType} ${type.javaType}Val = recordImplVersion.get${type.boxedType}Field(fieldName);
				BitSetExposed nullBitSet = nullStrips.get(fieldName);
				
				if(${type.javaType}Val == null) {
					nullBitSet.set(pos);
				} else {
					${type.javaType}ArrayIndex.get(fieldName)[pos] = ${type.javaType}Val;
					nullBitSet.clear(pos);
				}
			}
			break;
		</#list>
		default:
			throw new UnsupportedOperationException("Not supported for indexableField : " + indexableField);
		}
	}
	
	protected void touchArrayIndex(RecordImplVersion recordImplVersion, int pos, IndexableField<?> indexableField) {
		String fieldName = indexableField.getName();
		switch (indexableField.getDataType()) {
		<#list roadRunnerTypes.types as type>
		case ${type.caseCode}:
			{
				${type.boxedType} ${type.javaType}Val = recordImplVersion.get${type.boxedType}Field(fieldName);
				BitSetExposed nullBitSet = nullStrips.get(fieldName);
	
				if(${type.javaType}Val == null) {
					nullBitSet.set(pos);
				} else {
					${type.javaType}ArrayIndex.get(fieldName)[pos] = ${type.javaType}Val;
					nullBitSet.clear(pos);
				}
			}
			break;
		</#list>
		default:
			break;
		}
	}
	
	protected void deleteArrayIndex(int pos) {
		<#list roadRunnerTypes.types as type>
		for(Entry<String, ${type.javaType}[]> entry: ${type.javaType}ArrayIndex.entrySet()) {
			entry.getValue()[pos] = ${type.javaType}DefaultVal;	
		}
		</#list>
		
		for(BitSetExposed nullStrip : nullStrips.values()) {
			nullStrip.clear(pos);
		}
	}
}