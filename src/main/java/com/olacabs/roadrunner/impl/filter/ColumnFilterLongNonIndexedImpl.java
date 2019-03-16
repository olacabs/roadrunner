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

import java.util.List;

import com.olacabs.BitSetExposed;
import com.olacabs.roadrunner.api.Record;
import com.olacabs.roadrunner.api.RecordIndexes;
import com.olacabs.roadrunner.api.exceptions.CorruptedIndexException;
import com.olacabs.roadrunner.api.filter.ColumnFilterLong;
import com.olacabs.roadrunner.api.filter.ColumnFilterString.Operation;
import com.olacabs.roadrunner.impl.BitsetCache;
import com.olacabs.roadrunner.impl.RecordHolder;
import com.olacabs.roadrunner.impl.RecordIndexesImpl;
import com.olacabs.roadrunner.monitor.RoadRunnerMetricFactory;
import com.olacabs.roadrunner.utils.Logger;
import com.olacabs.roadrunner.utils.LoggerFactory;
import com.olacabs.roadrunner.utils.RoadRunnerUtils;

/**
 * Don't Edit this code, it is generated using templates
 */
public class ColumnFilterLongNonIndexedImpl implements ColumnFilterLong {

	String[] partitionBucketIds;
	String fieldName;
	String[] fieldNameTokens;
	BitSetExposed[] availablePosBitSets;
	RecordHolder[][] recordHolders;
	BitsetCache bitsCache;
	
	private static final RoadRunnerMetricFactory metrics = RoadRunnerMetricFactory.getInstance();
	private static final Logger logger = LoggerFactory.getLogger(ColumnFilterLongNonIndexedImpl.class);
	
	public ColumnFilterLongNonIndexedImpl(String[] partitionBucketIds, String fieldName, RecordHolder[][] recordHolders, BitSetExposed[] availablePosBitSets, BitsetCache bitsCache) {
		super();
		this.partitionBucketIds = partitionBucketIds;
		this.fieldName = fieldName;
		this.fieldNameTokens = fieldName.split("\\.");
		this.recordHolders = recordHolders;
		this.availablePosBitSets = availablePosBitSets;
		this.bitsCache = bitsCache;
	}

	private RecordIndexes opwork(Operation operation, long... values) {
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
	        int recordI, final long word, final int bitStartIndex, final int bitEndIndex, final long... values) {
		
		for (int bitIndex = bitStartIndex; bitIndex < bitEndIndex; bitIndex++) {
			boolean bitVal = ((word & (1L << bitIndex)) != 0);
			if (bitVal) {

				Record record = recordHolderArr[bitIndex].get(bucketId, bitIndex);

				if(record == null){

					intermediateBitSet.clear(bitIndex);

				} else {
					Object valueObj = record.getTokenizedField(fieldNameTokens);
					validateDataType(record, valueObj);
					switch (operation) {

					case GREATER_THAN:
						callbackGreaterThan(intermediateBitSet, bitIndex, valueObj, values[0]);
						break;

					case GREATER_THAN_EQUAL_TO:
						callbackGreaterThanEqualTo(intermediateBitSet, bitIndex, valueObj, values[0]);
						break;

					case LESSER_THAN:
						callbackLessThan(intermediateBitSet, bitIndex, valueObj, values[0]);
						break;

					case LESSER_THAN_EQUAL_TO:
						callbackLessThanEqualTo(intermediateBitSet, bitIndex, valueObj, values[0]);
						break;

					case EQUAL_TO:
						callbackEqualTo(intermediateBitSet, bitIndex, valueObj, values);
						break;

					case RANGE:
						callbackRange(intermediateBitSet, bitIndex, valueObj, values[0], values[1]);
						break;

					case MISSING:
						callbackMissing(intermediateBitSet, bitIndex, valueObj);
						break;

					case EXISTS:
						callbackExists(intermediateBitSet, bitIndex, valueObj);
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
		if(valueObj instanceof Long) return;
		
		if(valueObj instanceof List) {
			boolean valid = true;
			
			List<Object> values = (List<Object>) valueObj;
			for(Object value : values) {
				if(! (value instanceof Long)) {
					valid = false;
					break;
				}
			}
			
			if(valid) return;
		}
		
		metrics.increment("searcher_invalid_Long_data_type", 1);
		logger.error("valueObj {}, class : {} is not instance of Long or List of Long"
				+ " for fieldName : {}, Record : {}", valueObj, valueObj.getClass(), fieldName, record);
		throw new CorruptedIndexException("Data type of field Name : " + fieldName 
				+ " is not Long or List of Long for Record Id : " + record.getId());
	}

	private void callbackEqualTo(BitSetExposed intermediateBits, int bitIndex, Object valueObj, long... compareValues) {
		if(valueObj == null) {
			intermediateBits.clear(bitIndex);
			return;
		}
		boolean valueMatch = false;
		if(valueObj instanceof Long) {
			for(long compareValue : compareValues) {
				if(compareValue == ((Long) valueObj).longValue()) {
					valueMatch = true;
					break;
				}
			}
		} else {
			List<Long> values = (List<Long>) valueObj;
		
			for(Long value : values) {
				for(long compareValue : compareValues) {
					if(value != null && compareValue == value.longValue()) {
						valueMatch = true;
						break;
					}
				}
				if(valueMatch) break;
			}
		}
		if( ! valueMatch) {
			intermediateBits.clear(bitIndex);
		}
	}
	
	private void callbackGreaterThan(BitSetExposed intermediateBits, int bitIndex, Object valueObj, long compareValue) {
		if(valueObj == null) {
			intermediateBits.clear(bitIndex);
			return;
		}
		
		boolean valueMatch = false;
		if(valueObj instanceof Long) {
			if(((Long) valueObj).longValue() > compareValue) {
				valueMatch = true;
			}
		
		} else {
			List<Long> values = (List<Long>) valueObj;
			
			for(Long value : values) {
				if(value != null && value.longValue() > compareValue) {
					valueMatch = true;
					break;
				}
			}
		}
		if( ! valueMatch) {
			intermediateBits.clear(bitIndex);
		}
	}
	
	private void callbackGreaterThanEqualTo(BitSetExposed intermediateBits, int bitIndex, Object valueObj, long compareValue) {
		if(valueObj == null) {
			intermediateBits.clear(bitIndex);
			return;
		}
		
		boolean valueMatch = false;
		if(valueObj instanceof Long) {
			if(((Long) valueObj).longValue() >= compareValue) {
				valueMatch = true;
			}
		
		} else {
			List<Long> values = (List<Long>) valueObj;
			
			for(Long value : values) {
				if(value != null && value.longValue() >= compareValue) {
					valueMatch = true;
					break;
				}
			}
		}
		if( ! valueMatch) {
			intermediateBits.clear(bitIndex);
		}
	}
	
	private void callbackLessThan(BitSetExposed intermediateBits, int bitIndex, Object valueObj, long compareValue) {
		if(valueObj == null) {
			intermediateBits.clear(bitIndex);
			return;
		}
		
		boolean valueMatch = false;
		if(valueObj instanceof Long) {
			if(((Long) valueObj).longValue() < compareValue) {
				valueMatch = true;
			}
		
		} else {
			List<Long> values = (List<Long>) valueObj;
			
			for(Long value : values) {
				if(value != null && value.longValue() < compareValue) {
					valueMatch = true;
					break;
				}
			}
		}
		if( ! valueMatch) {
			intermediateBits.clear(bitIndex);
		}
	}
	
	private void callbackLessThanEqualTo(BitSetExposed intermediateBits, int bitIndex, Object valueObj, long compareValue) {
		if(valueObj == null) {
			intermediateBits.clear(bitIndex);
			return;
		}
		
		boolean valueMatch = false;
		if(valueObj instanceof Long) {
			if(((Long) valueObj).longValue() <= compareValue) {
				valueMatch = true;
			}
		
		} else {
			List<Long> values = (List<Long>) valueObj;
			
			for(Long value : values) {
				if(value != null && value.longValue() <= compareValue) {
					valueMatch = true;
					break;
				}
			}
		}
		if( ! valueMatch) {
			intermediateBits.clear(bitIndex);
		}
	}
	
	private void callbackRange(BitSetExposed intermediateBits, int bitIndex, Object valueObj, long minValue, long maxValue) {
		if(valueObj == null) {
			intermediateBits.clear(bitIndex);
			return;
		}
		
		boolean valueMatch = false;
		if(valueObj instanceof Long) {
			long value = ((Long) valueObj).longValue();
			if(value >= minValue && value <= maxValue) {
				valueMatch = true;
			}
		
		} else {
			List<Long> values = (List<Long>) valueObj;
			
			for(Long value : values) {
				if(value != null) {
					long nativeValue = value.longValue();
					if(nativeValue >= minValue && nativeValue <= maxValue) {
						valueMatch = true;
						break;
					}
				}
			}
		}
		if( ! valueMatch) {
			intermediateBits.clear(bitIndex);
		}
	}

	private void callbackMissing(BitSetExposed intermediateBits, int bitIndex, Object valueObj) {
		if(valueObj == null) return;
		
		if(valueObj instanceof List) {
			
			List<Long> values = (List<Long>) valueObj;
			if(values.isEmpty()) return;
			
			boolean validMatch = true;
			for(Long value : values) {
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
			List<Long> values = (List<Long>) valueObj;
			if(values.isEmpty()) {
				intermediateBits.clear(bitIndex);
				return;
			}
			
			boolean validMatch = false;
			for(Long value : values) {
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
	
	@Override
	public final RecordIndexes greaterThan(long value) {
		RoadRunnerUtils.validateCompareValues(fieldName, value);
		return opwork(Operation.GREATER_THAN, value);
	}

	@Override
	public final RecordIndexes lessThan(long value) {
		RoadRunnerUtils.validateCompareValues(fieldName, value);
		return opwork(Operation.LESSER_THAN, value);
	}

	@Override
	public final RecordIndexes greaterThanEqualTo(long value) {
		RoadRunnerUtils.validateCompareValues(fieldName, value);
		return opwork(Operation.GREATER_THAN_EQUAL_TO, value);
	}

	@Override
	public final RecordIndexes lessThanEqualTo(long value) {
		RoadRunnerUtils.validateCompareValues(fieldName, value);
		return opwork(Operation.LESSER_THAN_EQUAL_TO, value);
	}

	@Override
	public final RecordIndexes equalTo(long value) {
		RoadRunnerUtils.validateCompareValues(fieldName, value);
		return opwork(Operation.EQUAL_TO, value);
	}

	@Override
	public final RecordIndexes equalTo(long[] values) {
		RoadRunnerUtils.validateCompareValues(fieldName, values);
		return opwork(Operation.EQUAL_TO, values);
	}

	@Override
	public final RecordIndexes range(long minValue, long maxValue) {
		long[] values = new long[]{minValue, maxValue};
		RoadRunnerUtils.validateCompareValues(fieldName, values);
		return opwork(Operation.RANGE, values);
	}
	
	@Override
	public final RecordIndexes missing() {
		return opwork(Operation.MISSING, null);
	}
	
	@Override
	public final RecordIndexes exists() {
		return opwork(Operation.EXISTS, null);
	}
}

