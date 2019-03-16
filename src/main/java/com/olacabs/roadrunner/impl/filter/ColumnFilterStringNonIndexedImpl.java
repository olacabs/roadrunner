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

import java.util.List;

import com.olacabs.BitSetExposed;
import com.olacabs.roadrunner.api.Record;
import com.olacabs.roadrunner.api.RecordIndexes;
import com.olacabs.roadrunner.api.exceptions.CorruptedIndexException;
import com.olacabs.roadrunner.api.filter.ColumnFilterString;
import com.olacabs.roadrunner.impl.BitsetCache;
import com.olacabs.roadrunner.impl.RecordHolder;
import com.olacabs.roadrunner.impl.RecordIndexesImpl;
import com.olacabs.roadrunner.monitor.RoadRunnerMetricFactory;
import com.olacabs.roadrunner.utils.Logger;
import com.olacabs.roadrunner.utils.LoggerFactory;
import com.olacabs.roadrunner.utils.RoadRunnerUtils;


public class ColumnFilterStringNonIndexedImpl implements ColumnFilterString {

	String[] partitionBucketIds;
	String fieldName;
	String[] fieldNameTokens;
	BitSetExposed[] availablePosBitSets;
	RecordHolder[][] recordHolders;
	BitsetCache bitsCache;
	
	private static final RoadRunnerMetricFactory metrics = RoadRunnerMetricFactory.getInstance();
	private static final Logger logger = LoggerFactory.getLogger(ColumnFilterStringNonIndexedImpl.class);
	
	public ColumnFilterStringNonIndexedImpl(String[] partitionBucketIds, String fieldName, RecordHolder[][] recordHolders, BitSetExposed[] availablePosBitSets, BitsetCache bitsCache) {
		super();
		this.partitionBucketIds = partitionBucketIds;
		this.fieldName = fieldName;
		this.fieldNameTokens = fieldName.split("\\.");
		this.recordHolders = recordHolders;
		this.availablePosBitSets = availablePosBitSets;
		this.bitsCache = bitsCache;
	}

	private RecordIndexes opwork(Operation operation, String... values) {

		int availablePosBitSetsT = availablePosBitSets.length;
		BitSetExposed[] intermediateBits = new BitSetExposed[availablePosBitSetsT];

		for (int i = 0; i < availablePosBitSetsT; i++) {

			intermediateBits[i] = this.bitsCache.take();
			BitSetExposed availablePosBitSet = availablePosBitSets[i];
			intermediateBits[i].or(availablePosBitSet);
			RecordHolder[] recordHolderArr = recordHolders[i];
			String bucketId = partitionBucketIds[i];

			if(availablePosBitSet.nextSetBit(0) >= 0) {

				int recordsMaxCapacity = recordHolderArr.length;
				long[] words = intermediateBits[i].toLongArrayExposed();
				int wordsT = words.length;
				int recordsT = intermediateBits[i].cardinality();
				int recordI = 0;
				
				int maxWordIndex = (recordsMaxCapacity / 64) + 1;
				for(int wordIndex = 0; wordIndex < maxWordIndex; wordIndex++) {
					if (wordIndex >= wordsT) break;
					long word = words[wordIndex];
					int bitStartIndex = wordIndex * 64;
					int bitEndIndex =  bitStartIndex + 64;
					if(bitEndIndex > recordsMaxCapacity) bitEndIndex = recordsMaxCapacity;

					recordI = operate(operation, intermediateBits[i], recordHolderArr, bucketId, recordsT, recordI, word, bitStartIndex, bitEndIndex,
					        values);
					if (recordI >= recordsT) break;
				}
			}
		}
		return new RecordIndexesImpl(intermediateBits, availablePosBitSets, bitsCache);
	}

	private final int operate(final Operation operation, BitSetExposed intermediateBitSet, final RecordHolder[] recordHolderArr, final String bucketId, final int recordsT,
	        int recordI, final long word, final int bitStartIndex, final int bitEndIndex, final String... values) {
		
		for (int bitIndex = bitStartIndex; bitIndex < bitEndIndex; bitIndex++) {
			boolean bitVal = ((word & (1L << bitIndex)) != 0);
			if (bitVal) {

				Record record = recordHolderArr[bitIndex].get(bucketId, bitIndex);

				if(record == null) {

					intermediateBitSet.clear(bitIndex);

				} else {

					Object valueObj = record.getTokenizedField(fieldNameTokens);
					validateDataType(record, valueObj);

					switch (operation) {

					case EQUALS:
						callbackEquals(intermediateBitSet, bitIndex, valueObj, values[0]);
						break;

					case EQUALS_IGNORE_CASE:
						callbackEqualsIgnoreCase(intermediateBitSet, bitIndex, valueObj, values[0]);
						break;

					case IN:
						callbackIn(intermediateBitSet, bitIndex, valueObj, false, values);
						break;
						
					case IN_IGNORE_CASE:
						callbackIn(intermediateBitSet, bitIndex, valueObj, true, values);
						break;

					case NOT_EQUALS:
						callbackNotEquals(intermediateBitSet, bitIndex, valueObj, values[0]);
						break;

					case NOT_EQUALS_IGNORE_CASE:
						callbackNotEqualsIgnoreCase(intermediateBitSet, bitIndex, valueObj, values[0]);
						break;

					case NOT_IN:
						callbackNotIn(intermediateBitSet, bitIndex, valueObj, false, values);
						break;
						
					case NOT_IN_IGNORE_CASE:
						callbackNotIn(intermediateBitSet, bitIndex, valueObj, true, values);
						break;

					case MISSING:
						callbackMissing(intermediateBitSet, bitIndex, valueObj);
						break;

					case EXISTS:
						callbackExists(intermediateBitSet, bitIndex, valueObj);
						break;

					case PREFIX:
						callbackPrefix(intermediateBitSet, bitIndex, valueObj, values[0]);
						break;

					default:
						break;
					}
				
				}
				recordI++;						
			} 
			if (recordI >= recordsT) break;
		}
		return recordI;
	}

	public void validateDataType(Record record, Object valueObj) {

		if(valueObj == null) return;
		if(valueObj instanceof String) return;
		
		if(valueObj instanceof List) {
			boolean valid = true;
			
			List<Object> values = (List<Object>) valueObj;
			for(Object value : values) {
				if(! (value instanceof String)) {
					valid = false;
					break;
				}
			}
			
			if(valid) return;
		}
		
		metrics.increment("searcher_invalid_String_data_type", 1);
		logger.error("valueObj {}, class : {} is not instance of String or List of String"
				+ " for fieldName : {}, Record : {}", valueObj, valueObj.getClass(), fieldName, record);
		throw new CorruptedIndexException("Data type of field Name : " + fieldName 
				+ " is not String or List of String for Record Id : " + record.getId());
	}

	private void callbackEquals(BitSetExposed intermediateBits, int bitIndex, Object valueObj, String compareValue) {
		if(valueObj == null) {
			intermediateBits.clear(bitIndex);
			return;
		}
		boolean valueMatch = false;
		if(valueObj instanceof String) {
			if(compareValue.equals((String) valueObj)) {
				valueMatch = true;
			}
		
		} else {
			List<String> values = (List<String>) valueObj;
		
			for(String value : values) {
				if(compareValue.equals(value)) {
					valueMatch = true;
					break;
				}
			}
		}
		if( ! valueMatch) {
			intermediateBits.clear(bitIndex);
		}
	}
	
	private void callbackEqualsIgnoreCase(BitSetExposed intermediateBits, int bitIndex, Object valueObj, String compareValue) {

		if(valueObj == null) {
			intermediateBits.clear(bitIndex);
			return;
		}
		
		boolean valueMatch = false;
		if(valueObj instanceof String) {
			if(compareValue.equalsIgnoreCase((String) valueObj)) {
				valueMatch = true;
			}
		
		} else {
			List<String> values = (List<String>) valueObj;
		
			for(String value : values) {
				if(compareValue.equalsIgnoreCase(value)) {
					valueMatch = true;
					break;
				}
			}
		}
		if( ! valueMatch) {
			intermediateBits.clear(bitIndex);
		}
	}
	
	private void callbackIn(BitSetExposed intermediateBits, int bitIndex, Object valueObj, boolean ignoreCase, String... compareValues) {

		if(valueObj == null) {
			intermediateBits.clear(bitIndex);
			return;
		}
		
		boolean valueMatch = false;
		if(valueObj instanceof String) {
			String value = (String) valueObj;
			for(String compareValue : compareValues) {
				valueMatch = ignoreCase ? compareValue.equalsIgnoreCase(value) : compareValue.equals(value);
				if(valueMatch) break;
			}
		} else {
			List<String> values = (List<String>) valueObj;
		
			for(String value : values) {
				for(String compareValue : compareValues) {
					valueMatch = ignoreCase ? compareValue.equalsIgnoreCase(value) : compareValue.equals(value);
					if(valueMatch) break;
				}
				if(valueMatch) break;
			}
		}
		if( ! valueMatch) {
			intermediateBits.clear(bitIndex);
		}
	}
	
	private void callbackNotEquals(BitSetExposed intermediateBits, int bitIndex, Object valueObj, String compareValue) {

		if(valueObj == null) return;
		
		boolean valueMatch = false;
		if(valueObj instanceof String) {
			if(compareValue.equals((String) valueObj)) {
				valueMatch = true;
			}
		
		} else {
			List<String> values = (List<String>) valueObj;
			if(values.isEmpty()) {
				valueMatch = true;
			
			} else {
				for(String value : values) {
					if(compareValue.equals(value)) {
						valueMatch = true;
						break;
					}
				}
			}
		}
		if(valueMatch) {
			intermediateBits.clear(bitIndex);
		}
	}
	
	private void callbackNotEqualsIgnoreCase(BitSetExposed intermediateBits, int bitIndex, Object valueObj, String compareValue) {
		if(valueObj == null) return;
		
		boolean valueMatch = false;
		if(valueObj instanceof String) {
			if(compareValue.equalsIgnoreCase((String) valueObj)) {
				valueMatch = true;
			}
		
		} else {
			List<String> values = (List<String>) valueObj;
		
			if(values.isEmpty()) {
				valueMatch = true;
			
			} else {
				for(String value : values) {
					if(compareValue.equalsIgnoreCase(value)) {
						valueMatch = true;
						break;
					}
				}
			}
		}
		if(valueMatch) {
			intermediateBits.clear(bitIndex);
		}
	}
	
	private void callbackNotIn(BitSetExposed intermediateBits, int bitIndex, Object valueObj, boolean ignoreCase, String... compareValues) {
		if(valueObj == null) return;
		
		boolean valueMatch = false;
		if(valueObj instanceof String) {
			String value = (String) valueObj;
			for(String compareValue : compareValues) {
				valueMatch = ignoreCase ? compareValue.equalsIgnoreCase(value) : compareValue.equals(value);
				if(valueMatch) break;
			}
		} else {
			List<String> values = (List<String>) valueObj;
			
			if(values.isEmpty()) {
				valueMatch = true;
			
			} else {
				for(String value : values) {
					for(String compareValue : compareValues) {
						valueMatch = ignoreCase ? compareValue.equalsIgnoreCase(value) : compareValue.equals(value);
						if(valueMatch) break;
					}
					if(valueMatch) break;
				}
			}
		}
		if(valueMatch) {
			intermediateBits.clear(bitIndex);
		}
	}

	private void callbackMissing(BitSetExposed intermediateBits, int bitIndex, Object valueObj) {
		if(valueObj == null) return;
		
		if(valueObj instanceof List) {
			
			List<String> values = (List<String>) valueObj;
			if(values.isEmpty()) return;
			
			boolean validMatch = true;
			for(String value : values) {
				if(value != null) {
					validMatch = false;
					break;
				}
			}
			
			if(validMatch) return;
		}
		
		intermediateBits.clear(bitIndex);
	}

	private void callbackExists(BitSetExposed intermediateBits, int bitIndex, Object valueObj) {
		if(valueObj == null) {
			intermediateBits.clear(bitIndex);
			return;
		}
		
		if(valueObj instanceof List) {
			List<String> values = (List<String>) valueObj;
			if(values.isEmpty()) {
				intermediateBits.clear(bitIndex);
				return;
			}
			
			boolean validMatch = false;
			for(String value : values) {
				if(value != null) {
					validMatch = true;
				}
			}
			
			if( ! validMatch) {
				intermediateBits.clear(bitIndex);
				return;
			}
		}
	}

	private void callbackPrefix(BitSetExposed intermediateBits, int bitIndex, Object valueObj, String compareValue) {
		if(valueObj == null) {
			intermediateBits.clear(bitIndex);
			return;
		}
		boolean valueMatch = false;
		if(valueObj instanceof String) {
			if(((String) valueObj).startsWith(compareValue)) {
				valueMatch = true;
			}

		} else {
			List<String> values = (List<String>) valueObj;

			for(String value : values) {
				if(value != null && value.startsWith(compareValue)) {
					valueMatch = true;
					break;
				}
			}
		}
		if( ! valueMatch) {
			intermediateBits.clear(bitIndex);
		}	
	}
	
	@Override
	public RecordIndexes equals(String value) {
		RoadRunnerUtils.validateCompareValues(fieldName, value);
		return opwork(Operation.EQUALS, value);
	}

	@Override
	public RecordIndexes equalsIgnoreCase(String value) {
		RoadRunnerUtils.validateCompareValues(fieldName, value);
		return opwork(Operation.EQUALS_IGNORE_CASE, value);
	}

	@Override
	public RecordIndexes in(String[] values) {
		return in(values, false);
	}
	
	@Override
	public RecordIndexes in(String[] values, boolean ignoreCase) {
		RoadRunnerUtils.validateCompareValues(fieldName, values);
		if(ignoreCase) {
			return opwork(Operation.IN_IGNORE_CASE, values);
		} else {
			return opwork(Operation.IN, values);
		}
	}

	@Override
	public RecordIndexes notEquals(String value) {
		RoadRunnerUtils.validateCompareValues(fieldName, value);
		return opwork(Operation.NOT_EQUALS, value);
	}

	@Override
	public RecordIndexes notEqualsIgnoreCase(String value) {
		RoadRunnerUtils.validateCompareValues(fieldName, value);
		return opwork(Operation.NOT_EQUALS_IGNORE_CASE, value);
	}

	@Override
	public RecordIndexes notIn(String[] values) {
		return notIn(values, false);
	}
	
	@Override
	public RecordIndexes notIn(String[] values, boolean ignoreCase) {
		RoadRunnerUtils.validateCompareValues(fieldName, values);
		
		if(ignoreCase) {
			return opwork(Operation.NOT_IN_IGNORE_CASE, values);
		} else {
			return opwork(Operation.NOT_IN, values);
		}
	}

	@Override
	public RecordIndexes missing() {
		return opwork(Operation.MISSING, null);
	}

	@Override
	public RecordIndexes exists() {
		return opwork(Operation.EXISTS, null);
	}

	@Override
	public RecordIndexes prefix(String value) {
		RoadRunnerUtils.validateCompareValues(fieldName, value);
		return opwork(Operation.PREFIX, value);
	}
}
