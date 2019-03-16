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
import com.olacabs.roadrunner.api.filter.ColumnFilterBoolean;
import com.olacabs.roadrunner.impl.BitsetCache;
import com.olacabs.roadrunner.impl.RecordIndexesImpl;
import com.olacabs.roadrunner.impl.RecordHolder;
import com.olacabs.roadrunner.monitor.RoadRunnerMetricFactory;
import com.olacabs.roadrunner.utils.Logger;
import com.olacabs.roadrunner.utils.LoggerFactory;

public class ColumnFilterBooleanNonIndexedImpl implements ColumnFilterBoolean {

	String[] partitionBucketIds;
	String fieldName;
	String[] fieldNameTokens;
	BitSetExposed[] availablePosBitSets;
	RecordHolder[][] recordHolders;
	BitsetCache bitsCache;
	
	private static final RoadRunnerMetricFactory metrics = RoadRunnerMetricFactory.getInstance();
	private static final Logger logger = LoggerFactory.getLogger(ColumnFilterBooleanNonIndexedImpl.class);
	
	private enum Operation {
		IS_TRUE, IS_FALSE, IS_NULL
	}

	public ColumnFilterBooleanNonIndexedImpl(String[] partitionBucketIds, String fieldName, RecordHolder[][] recordHolders, BitSetExposed[] availablePosBitSets, BitsetCache bitsCache) {
		super();
		this.partitionBucketIds = partitionBucketIds;
		this.fieldName = fieldName;
		this.fieldNameTokens = fieldName.split("\\.");
		this.recordHolders = recordHolders;
		this.availablePosBitSets = availablePosBitSets;
		this.bitsCache = bitsCache;
	}
	
	private RecordIndexes opwork(Operation operation) {
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
					
					recordI = operate(operation, intermediateBits[i], recordHolderArr, bucketId, recordsT, recordI, word, bitStartIndex, bitEndIndex);
					if (recordI >= recordsT) break;
				}
			}
		}
		return new RecordIndexesImpl(intermediateBits, availablePosBitSets, bitsCache);
	}

	private int operate(final Operation operation, BitSetExposed intermediateBitSet, final RecordHolder[] recordHolderArr, final String bucketId, final int recordsT,
	        int recordI, final long word, final int bitStartIndex, final int bitEndIndex) {
		
		for (int bitIndex = bitStartIndex; bitIndex < bitEndIndex; bitIndex++) {
			boolean bitVal = ((word & (1L << bitIndex)) != 0);
			if (bitVal) {

				Record record = recordHolderArr[bitIndex].get(bucketId, bitIndex);

				if ( null == record) {
					intermediateBitSet.clear(bitIndex);
				} else {
					Object valueObj = record.getTokenizedField(fieldNameTokens);
					validateDataType(record, valueObj);

					switch (operation) {

					case IS_TRUE:
						callbackIsTrue(intermediateBitSet, bitIndex, valueObj);
						break;

					case IS_FALSE:
						callbackIsFalse(intermediateBitSet, bitIndex, valueObj);
						break;

					case IS_NULL:
						callbackIsNull(intermediateBitSet, bitIndex, valueObj);
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

	private void callbackIsTrue(BitSetExposed intermediateBits, int bitIndex, Object valueObj) {

		if(valueObj == null) {
			intermediateBits.clear(bitIndex);
			return;
		}

		boolean valueMatch = false;
		if(valueObj instanceof Boolean) {
			if((Boolean) valueObj) {
				valueMatch = true;
			}
		
		} else {
			List<Boolean> values = (List<Boolean>) valueObj;
		
			for(Boolean value : values) {
				if(value != null && value) {
					valueMatch = true;
					break;
				}
			}
		}
		if( ! valueMatch) {
			intermediateBits.clear(bitIndex);
		}
	}

	private void callbackIsFalse(BitSetExposed intermediateBits, int bitIndex, Object valueObj) {
		if(valueObj == null) {
			intermediateBits.clear(bitIndex);
			return;
		}
		boolean valueMatch = false;
		if(valueObj instanceof Boolean) {
			if(! (Boolean) valueObj) {
				valueMatch = true;
			}
		
		} else {
			List<Boolean> values = (List<Boolean>) valueObj;
		
			for(Boolean value : values) {
				if(value != null && ! value) {
					valueMatch = true;
					break;
				}
			}
		}
		if( ! valueMatch) {
			intermediateBits.clear(bitIndex);
		}
	}

	private void callbackIsNull(BitSetExposed intermediateBits, int bitIndex, Object valueObj) {
		if(valueObj == null) return;
		
		if(valueObj instanceof List) {
			List<Boolean> values = (List<Boolean>) valueObj;
			if(values.isEmpty()) return;
			
			boolean nullMatch = true;
			for(Boolean value : values) {
				if(value != null) {
					nullMatch = false;
					break;
				}
			}
			
			if(nullMatch) return;
		}
		
		intermediateBits.clear(bitIndex);
	}

	public void validateDataType(Record record, Object valueObj) {
		if(valueObj == null) return;
		if(valueObj instanceof Boolean) return;
		
		if(valueObj instanceof List) {
			boolean valid = true;
			
			List<Boolean> values = (List<Boolean>) valueObj;
			for(Object value : values) {
				if(! (value instanceof Boolean)) {
					valid = false;
					break;
				}
			}
			
			if(valid) return;
		}
		
		metrics.increment("searcher_invalid_Boolean_data_type", 1);
		logger.error("valueObj {}, class : {} is not instance of Boolean or List of Boolean"
				+ " for fieldName : {}, Record : {}", valueObj, valueObj.getClass(), fieldName, record);
		throw new CorruptedIndexException("Data type of field Name : " + fieldName 
				+ " is not Boolean or List of Boolean for Record Id : " + record.getId());
	}

	@Override
	public RecordIndexes isTrue() {
		return opwork(Operation.IS_TRUE);
	}

	@Override
	public RecordIndexes isFalse() {
		return opwork(Operation.IS_FALSE);
	}

	@Override
	public RecordIndexes isNull() {
		return opwork(Operation.IS_NULL);
	}
}
