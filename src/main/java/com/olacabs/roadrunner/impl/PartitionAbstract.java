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
public class PartitionAbstract {

	protected Map<String, BitSetExposed> nullStrips;
	
	protected Map<String, short[]> shortArrayIndex;
	protected Map<String, int[]> intArrayIndex;
	protected Map<String, float[]> floatArrayIndex;
	protected Map<String, long[]> longArrayIndex;
	protected Map<String, double[]> doubleArrayIndex;
	protected Map<String, String[]> StringArrayIndex;

	protected int capacity = 0;

	private static final short shortDefaultVal = -1;
	private static final int intDefaultVal = -1;
	private static final float floatDefaultVal = -1;
	private static final long longDefaultVal = -1;
	private static final double doubleDefaultVal = -1;
	private static final String StringDefaultVal = null;

	protected void expandMaps(int currentCapacity, int newCapacity) {
		
//		Expanding Array index
		Map<String, short[]> newShortArrayIndex = new HashMap<String, short[]>(shortArrayIndex);
        {
            for (Entry<String, short[]> entry : newShortArrayIndex.entrySet()) {
            	short[] shortArray = new short[newCapacity];
                System.arraycopy(entry.getValue(), 0, shortArray, 0, currentCapacity);
                Arrays.fill(shortArray, currentCapacity, newCapacity, shortDefaultVal);
                entry.setValue(shortArray);
            }
            Map<String, short[]> tmp = shortArrayIndex;
            shortArrayIndex = newShortArrayIndex;
            tmp.clear();
        }
//		Expanding Array index
		Map<String, int[]> newIntegerArrayIndex = new HashMap<String, int[]>(intArrayIndex);
        {
            for (Entry<String, int[]> entry : newIntegerArrayIndex.entrySet()) {
            	int[] intArray = new int[newCapacity];
                System.arraycopy(entry.getValue(), 0, intArray, 0, currentCapacity);
                Arrays.fill(intArray, currentCapacity, newCapacity, intDefaultVal);
                entry.setValue(intArray);
            }
            Map<String, int[]> tmp = intArrayIndex;
            intArrayIndex = newIntegerArrayIndex;
            tmp.clear();
        }
//		Expanding Array index
		Map<String, float[]> newFloatArrayIndex = new HashMap<String, float[]>(floatArrayIndex);
        {
            for (Entry<String, float[]> entry : newFloatArrayIndex.entrySet()) {
            	float[] floatArray = new float[newCapacity];
                System.arraycopy(entry.getValue(), 0, floatArray, 0, currentCapacity);
                Arrays.fill(floatArray, currentCapacity, newCapacity, floatDefaultVal);
                entry.setValue(floatArray);
            }
            Map<String, float[]> tmp = floatArrayIndex;
            floatArrayIndex = newFloatArrayIndex;
            tmp.clear();
        }
//		Expanding Array index
		Map<String, long[]> newLongArrayIndex = new HashMap<String, long[]>(longArrayIndex);
        {
            for (Entry<String, long[]> entry : newLongArrayIndex.entrySet()) {
            	long[] longArray = new long[newCapacity];
                System.arraycopy(entry.getValue(), 0, longArray, 0, currentCapacity);
                Arrays.fill(longArray, currentCapacity, newCapacity, longDefaultVal);
                entry.setValue(longArray);
            }
            Map<String, long[]> tmp = longArrayIndex;
            longArrayIndex = newLongArrayIndex;
            tmp.clear();
        }
//		Expanding Array index
		Map<String, double[]> newDoubleArrayIndex = new HashMap<String, double[]>(doubleArrayIndex);
        {
            for (Entry<String, double[]> entry : newDoubleArrayIndex.entrySet()) {
            	double[] doubleArray = new double[newCapacity];
                System.arraycopy(entry.getValue(), 0, doubleArray, 0, currentCapacity);
                Arrays.fill(doubleArray, currentCapacity, newCapacity, doubleDefaultVal);
                entry.setValue(doubleArray);
            }
            Map<String, double[]> tmp = doubleArrayIndex;
            doubleArrayIndex = newDoubleArrayIndex;
            tmp.clear();
        }
//		Expanding Array index
		Map<String, String[]> newStringArrayIndex = new HashMap<String, String[]>(StringArrayIndex);
        {
            for (Entry<String, String[]> entry : newStringArrayIndex.entrySet()) {
            	String[] StringArray = new String[newCapacity];
                System.arraycopy(entry.getValue(), 0, StringArray, 0, currentCapacity);
                Arrays.fill(StringArray, currentCapacity, newCapacity, StringDefaultVal);
                entry.setValue(StringArray);
            }
            Map<String, String[]> tmp = StringArrayIndex;
            StringArrayIndex = newStringArrayIndex;
            tmp.clear();
        }

		capacity = newCapacity;

	}
	
	protected void updateArrayIndex(RecordImplVersion recordImplVersion, IndexableField<?> indexableField, int size) {
		int pos = recordImplVersion.getPOS();
		String fieldName = indexableField.getName();
		
		switch (indexableField.getDataType()) {
		case SHORT:
			{
				if(shortArrayIndex.get(fieldName) == null) {
					shortArrayIndex.put(fieldName, new short[size]);
				} 
				Short shortVal = recordImplVersion.getShortField(fieldName);
				BitSetExposed nullBitSet = nullStrips.get(fieldName);
				
				if(shortVal == null) {
					nullBitSet.set(pos);
				} else {
					shortArrayIndex.get(fieldName)[pos] = shortVal;
					nullBitSet.clear(pos);
				}
			}
			break;
		case INT:
			{
				if(intArrayIndex.get(fieldName) == null) {
					intArrayIndex.put(fieldName, new int[size]);
				} 
				Integer intVal = recordImplVersion.getIntegerField(fieldName);
				BitSetExposed nullBitSet = nullStrips.get(fieldName);
				
				if(intVal == null) {
					nullBitSet.set(pos);
				} else {
					intArrayIndex.get(fieldName)[pos] = intVal;
					nullBitSet.clear(pos);
				}
			}
			break;
		case FLOAT:
			{
				if(floatArrayIndex.get(fieldName) == null) {
					floatArrayIndex.put(fieldName, new float[size]);
				} 
				Float floatVal = recordImplVersion.getFloatField(fieldName);
				BitSetExposed nullBitSet = nullStrips.get(fieldName);
				
				if(floatVal == null) {
					nullBitSet.set(pos);
				} else {
					floatArrayIndex.get(fieldName)[pos] = floatVal;
					nullBitSet.clear(pos);
				}
			}
			break;
		case LONG:
			{
				if(longArrayIndex.get(fieldName) == null) {
					longArrayIndex.put(fieldName, new long[size]);
				} 
				Long longVal = recordImplVersion.getLongField(fieldName);
				BitSetExposed nullBitSet = nullStrips.get(fieldName);
				
				if(longVal == null) {
					nullBitSet.set(pos);
				} else {
					longArrayIndex.get(fieldName)[pos] = longVal;
					nullBitSet.clear(pos);
				}
			}
			break;
		case DOUBLE:
			{
				if(doubleArrayIndex.get(fieldName) == null) {
					doubleArrayIndex.put(fieldName, new double[size]);
				} 
				Double doubleVal = recordImplVersion.getDoubleField(fieldName);
				BitSetExposed nullBitSet = nullStrips.get(fieldName);
				
				if(doubleVal == null) {
					nullBitSet.set(pos);
				} else {
					doubleArrayIndex.get(fieldName)[pos] = doubleVal;
					nullBitSet.clear(pos);
				}
			}
			break;
		case STRING:
			{
				if(StringArrayIndex.get(fieldName) == null) {
					StringArrayIndex.put(fieldName, new String[size]);
				} 
				String StringVal = recordImplVersion.getStringField(fieldName);
				BitSetExposed nullBitSet = nullStrips.get(fieldName);
				
				if(StringVal == null) {
					nullBitSet.set(pos);
				} else {
					StringArrayIndex.get(fieldName)[pos] = StringVal;
					nullBitSet.clear(pos);
				}
			}
			break;
		default:
			throw new UnsupportedOperationException("Not supported for indexableField : " + indexableField);
		}
	}
	
	protected void touchArrayIndex(RecordImplVersion recordImplVersion, int pos, IndexableField<?> indexableField) {
		String fieldName = indexableField.getName();
		switch (indexableField.getDataType()) {
		case SHORT:
			{
				Short shortVal = recordImplVersion.getShortField(fieldName);
				BitSetExposed nullBitSet = nullStrips.get(fieldName);
	
				if(shortVal == null) {
					nullBitSet.set(pos);
				} else {
					shortArrayIndex.get(fieldName)[pos] = shortVal;
					nullBitSet.clear(pos);
				}
			}
			break;
		case INT:
			{
				Integer intVal = recordImplVersion.getIntegerField(fieldName);
				BitSetExposed nullBitSet = nullStrips.get(fieldName);
	
				if(intVal == null) {
					nullBitSet.set(pos);
				} else {
					intArrayIndex.get(fieldName)[pos] = intVal;
					nullBitSet.clear(pos);
				}
			}
			break;
		case FLOAT:
			{
				Float floatVal = recordImplVersion.getFloatField(fieldName);
				BitSetExposed nullBitSet = nullStrips.get(fieldName);
	
				if(floatVal == null) {
					nullBitSet.set(pos);
				} else {
					floatArrayIndex.get(fieldName)[pos] = floatVal;
					nullBitSet.clear(pos);
				}
			}
			break;
		case LONG:
			{
				Long longVal = recordImplVersion.getLongField(fieldName);
				BitSetExposed nullBitSet = nullStrips.get(fieldName);
	
				if(longVal == null) {
					nullBitSet.set(pos);
				} else {
					longArrayIndex.get(fieldName)[pos] = longVal;
					nullBitSet.clear(pos);
				}
			}
			break;
		case DOUBLE:
			{
				Double doubleVal = recordImplVersion.getDoubleField(fieldName);
				BitSetExposed nullBitSet = nullStrips.get(fieldName);
	
				if(doubleVal == null) {
					nullBitSet.set(pos);
				} else {
					doubleArrayIndex.get(fieldName)[pos] = doubleVal;
					nullBitSet.clear(pos);
				}
			}
			break;
		case STRING:
			{
				String StringVal = recordImplVersion.getStringField(fieldName);
				BitSetExposed nullBitSet = nullStrips.get(fieldName);
	
				if(StringVal == null) {
					nullBitSet.set(pos);
				} else {
					StringArrayIndex.get(fieldName)[pos] = StringVal;
					nullBitSet.clear(pos);
				}
			}
			break;
		default:
			break;
		}
	}
	
	protected void deleteArrayIndex(int pos) {
		for(Entry<String, short[]> entry: shortArrayIndex.entrySet()) {
			entry.getValue()[pos] = shortDefaultVal;	
		}
		for(Entry<String, int[]> entry: intArrayIndex.entrySet()) {
			entry.getValue()[pos] = intDefaultVal;	
		}
		for(Entry<String, float[]> entry: floatArrayIndex.entrySet()) {
			entry.getValue()[pos] = floatDefaultVal;	
		}
		for(Entry<String, long[]> entry: longArrayIndex.entrySet()) {
			entry.getValue()[pos] = longDefaultVal;	
		}
		for(Entry<String, double[]> entry: doubleArrayIndex.entrySet()) {
			entry.getValue()[pos] = doubleDefaultVal;	
		}
		for(Entry<String, String[]> entry: StringArrayIndex.entrySet()) {
			entry.getValue()[pos] = StringDefaultVal;	
		}
		
		for(BitSetExposed nullStrip : nullStrips.values()) {
			nullStrip.clear(pos);
		}
	}
}