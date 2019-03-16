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

import org.apache.commons.lang3.Validate;

import com.olacabs.BitSetExposed;
import com.olacabs.roadrunner.api.filter.ColumnFilterInteger;
import com.olacabs.roadrunner.api.filter.ColumnFilterString.Operation;
import com.olacabs.roadrunner.api.RecordIndexes;

import com.olacabs.roadrunner.impl.BitsetCache;
import com.olacabs.roadrunner.impl.RecordHolder;
import com.olacabs.roadrunner.impl.RecordIndexesImpl;
import com.olacabs.roadrunner.utils.RoadRunnerUtils;

/**
 * Don't Edit this code, it is generated using templates
 */
public class ColumnFilterIntegerUniqueImpl implements ColumnFilterInteger {
	
	String fieldName;
	int[][] columnDataArray;
	BitSetExposed[] nullStripArray;
	BitSetExposed[] availablePosBitSets;
	BitsetCache bitsCache;
	
	public ColumnFilterIntegerUniqueImpl(String fieldName, int[][] columnDataArray, BitSetExposed[] nullStripArray, BitSetExposed[] availablePosBitSets, BitsetCache bitsCache) {
		super();
		this.fieldName = fieldName;
		this.columnDataArray = columnDataArray;
		this.nullStripArray = nullStripArray;
		this.availablePosBitSets = availablePosBitSets;
		this.bitsCache = bitsCache;
	}

	private final void callBackGreaterThan(final int[] dataArray, final BitSetExposed intermediateBits, final int value) {
		
		int recordsMaxCapacity = dataArray.length;

		long[] words = intermediateBits.toLongArrayExposed();
		int wordsT = words.length;

		int recordsT = intermediateBits.cardinality();
		int recordI = 0;

		int maxWordIndex = (recordsMaxCapacity / 64) + 1;
		for(int wordIndex = 0; wordIndex < maxWordIndex; wordIndex++) {
			if (wordIndex >= wordsT) break;
			long word = words[wordIndex];
			int bitStartIndex = wordIndex * 64;
			int bitEndIndex =  bitStartIndex + 64;
			if(bitEndIndex > recordsMaxCapacity) bitEndIndex = recordsMaxCapacity;

			recordI = operateGreaterThan(dataArray, intermediateBits, recordsT, recordI, word, bitStartIndex, bitEndIndex, recordsMaxCapacity, value);
			if ( recordI >= recordsT) break;
		}
	} 
	
	private final int operateGreaterThan(final int[] dataArray, final BitSetExposed intermediateBits, final int recordsT, int recordI, final long word, 
			final int bitStartIndex, final int bitEndIndex, final int recordsMaxCapacity, final int value) {
		
		for (int bitIndex = bitStartIndex; bitIndex < bitEndIndex; bitIndex++) {
			boolean bitVal = ((word & (1L << bitIndex)) != 0);
			if (bitVal) {
				if(dataArray[bitIndex] <= value) {
					intermediateBits.clear(bitIndex);
				}
				recordI++;
			} 
			if ( recordI >= recordsT) {
				intermediateBits.clear(bitIndex + 1, recordsMaxCapacity);
				break;
			}
		}
		return recordI;
	}
	
	private final void callBackGreaterThanEqualTo(final int[] dataArray, final BitSetExposed intermediateBits, final int value) {
		int recordsMaxCapacity = dataArray.length;

		long[] words = intermediateBits.toLongArrayExposed();
		int wordsT = words.length;

		int recordsT = intermediateBits.cardinality();
		int recordI = 0;
		
		int maxWordIndex = (recordsMaxCapacity / 64) + 1;
		for(int wordIndex = 0; wordIndex < maxWordIndex; wordIndex++) {
			if (wordIndex >= wordsT) break;
			long word = words[wordIndex];
			int bitStartIndex = wordIndex * 64;
			int bitEndIndex =  bitStartIndex + 64;
			if(bitEndIndex > recordsMaxCapacity) bitEndIndex = recordsMaxCapacity;

			recordI = operateGreaterThanEqualTo(dataArray, intermediateBits, recordsT, recordI, word, bitStartIndex, bitEndIndex, recordsMaxCapacity, value);
			if ( recordI >= recordsT) break;
		}
	} 
	
	private final int operateGreaterThanEqualTo(final int[] dataArray, final BitSetExposed intermediateBits, final int recordsT, int recordI, final long word, 
			final int bitStartIndex, final int bitEndIndex, final int recordsMaxCapacity, final int value) {
		
		for (int bitIndex = bitStartIndex; bitIndex < bitEndIndex; bitIndex++) {
			boolean bitVal = ((word & (1L << bitIndex)) != 0);
			if (bitVal) {
				if(dataArray[bitIndex] < value) {
					intermediateBits.clear(bitIndex);
				}
				recordI++;
			} 
			if ( recordI >= recordsT) {
				intermediateBits.clear(bitIndex + 1, recordsMaxCapacity);
				break;
			}
		}
		return recordI;
	}
	
	private final void callBackLesserThan(final int[] dataArray, final BitSetExposed intermediateBits, final int value) {
		int recordsMaxCapacity = dataArray.length;

		long[] words = intermediateBits.toLongArrayExposed();
		int wordsT = words.length;

		int recordsT = intermediateBits.cardinality();
		int recordI = 0;
		
		int maxWordIndex = (recordsMaxCapacity / 64) + 1;
		for(int wordIndex = 0; wordIndex < maxWordIndex; wordIndex++) {
			if (wordIndex >= wordsT) break;
			long word = words[wordIndex];
			int bitStartIndex = wordIndex * 64;
			int bitEndIndex =  bitStartIndex + 64;
			if(bitEndIndex > recordsMaxCapacity) bitEndIndex = recordsMaxCapacity;

			recordI = operateLesserThan(dataArray, intermediateBits, recordsT, recordI, word, bitStartIndex, bitEndIndex, recordsMaxCapacity, value);
			if ( recordI >= recordsT) break;
		}
	} 
	
	private final int operateLesserThan(final int[] dataArray, final BitSetExposed intermediateBits, final int recordsT, int recordI, final long word, 
			final int bitStartIndex, final int bitEndIndex, final int recordsMaxCapacity, final int value) {
		
		for (int bitIndex = bitStartIndex; bitIndex < bitEndIndex; bitIndex++) {
			boolean bitVal = ((word & (1L << bitIndex)) != 0);
			if (bitVal) {
				if(dataArray[bitIndex] >= value) {
					intermediateBits.clear(bitIndex);
				}
				recordI++;
			} 
			if ( recordI >= recordsT) {
				intermediateBits.clear(bitIndex + 1, recordsMaxCapacity);
				break;
			}
		}
		return recordI;
	}
	
	private final void callBackLesserThanEqualTo(final int[] dataArray, final BitSetExposed intermediateBits, final int value) {
		int recordsMaxCapacity = dataArray.length;

		long[] words = intermediateBits.toLongArrayExposed();
		int wordsT = words.length;

		int recordsT = intermediateBits.cardinality();
		int recordI = 0;
		
		int maxWordIndex = (recordsMaxCapacity / 64) + 1;
		for(int wordIndex = 0; wordIndex < maxWordIndex; wordIndex++) {
			if (wordIndex >= wordsT) break;
			long word = words[wordIndex];
			int bitStartIndex = wordIndex * 64;
			int bitEndIndex =  bitStartIndex + 64;
			if(bitEndIndex > recordsMaxCapacity) bitEndIndex = recordsMaxCapacity;

			recordI = operateLesserThanEqualTo(dataArray, intermediateBits, recordsT, recordI, word, bitStartIndex, bitEndIndex, recordsMaxCapacity, value);
			if ( recordI >= recordsT) break;
		}
	} 
	
	private final int operateLesserThanEqualTo(final int[] dataArray, final BitSetExposed intermediateBits, final int recordsT, int recordI, final long word, 
			final int bitStartIndex, final int bitEndIndex, final int recordsMaxCapacity, final int value) {
		
		for (int bitIndex = bitStartIndex; bitIndex < bitEndIndex; bitIndex++) {
			boolean bitVal = ((word & (1L << bitIndex)) != 0);
			if (bitVal) {
				if(dataArray[bitIndex] > value) {
					intermediateBits.clear(bitIndex);
				}
				recordI++;
			} 
			if ( recordI >= recordsT) {
				intermediateBits.clear(bitIndex + 1, recordsMaxCapacity);
				break;
			}
		}
		return recordI;
	}
	
	private final void callBackEqualTo(final int[] dataArray, final BitSetExposed intermediateBits, final int value) {
		int recordsMaxCapacity = dataArray.length;

		long[] words = intermediateBits.toLongArrayExposed();
		int wordsT = words.length;

		int recordsT = intermediateBits.cardinality();
		int recordI = 0;
		
		int maxWordIndex = (recordsMaxCapacity / 64) + 1;
		for(int wordIndex = 0; wordIndex < maxWordIndex; wordIndex++) {
			if (wordIndex >= wordsT) break;
			long word = words[wordIndex];
			int bitStartIndex = wordIndex * 64;
			int bitEndIndex =  bitStartIndex + 64;
			if(bitEndIndex > recordsMaxCapacity) bitEndIndex = recordsMaxCapacity;

			recordI = operateEqualTo(dataArray, intermediateBits, recordsT, recordI, word, bitStartIndex, bitEndIndex, recordsMaxCapacity, value);
			if ( recordI >= recordsT) break;
		}
	} 
	
	private final int operateEqualTo(final int[] dataArray, final BitSetExposed intermediateBits, final int recordsT, int recordI, final long word, 
			final int bitStartIndex, final int bitEndIndex, final int recordsMaxCapacity, final int value) {
		
		for (int bitIndex = bitStartIndex; bitIndex < bitEndIndex; bitIndex++) {
			boolean bitVal = ((word & (1L << bitIndex)) != 0);
			if (bitVal) {
				if(dataArray[bitIndex] != value) {
					intermediateBits.clear(bitIndex);
				}
				recordI++;
			} 
			if ( recordI >= recordsT) {
				intermediateBits.clear(bitIndex + 1, recordsMaxCapacity);
				break;
			}
		}
		return recordI;
	}
	
	private final void callBackEqualTo(final int[] dataArray, final BitSetExposed intermediateBits, final int[] values) {
		int recordsMaxCapacity = dataArray.length;

		long[] words = intermediateBits.toLongArrayExposed();
		int wordsT = words.length;

		int recordsT = intermediateBits.cardinality();
		int recordI = 0;
		
		int maxWordIndex = (recordsMaxCapacity / 64) + 1;
		for(int wordIndex = 0; wordIndex < maxWordIndex; wordIndex++) {
			if (wordIndex >= wordsT) break;
			long word = words[wordIndex];
			int bitStartIndex = wordIndex * 64;
			int bitEndIndex =  bitStartIndex + 64;
			if(bitEndIndex > recordsMaxCapacity) bitEndIndex = recordsMaxCapacity;
			
			recordI = operateEqualTo(dataArray, intermediateBits, recordsT, recordI, word, bitStartIndex, bitEndIndex, recordsMaxCapacity, values);
			if ( recordI >= recordsT) break;
		}
	} 
	
	private final int operateEqualTo(final int[] dataArray, final BitSetExposed intermediateBits, final int recordsT, int recordI, final long word, 
			final int bitStartIndex, final int bitEndIndex, final int recordsMaxCapacity, final int[] values) {
		
		for (int bitIndex = bitStartIndex; bitIndex < bitEndIndex; bitIndex++) {
			boolean bitVal = ((word & (1L << bitIndex)) != 0);
			if (bitVal) {
				int storedVal = dataArray[bitIndex];
				int valuesT = values.length;
				boolean match = false;
				for(int i = 0; i < valuesT; i++) {
					if(storedVal == values[i]) {
						match = true;
						break;
					}
				}
				if(! match) {
					intermediateBits.clear(bitIndex);
				}
				recordI++;
			} 
			if ( recordI >= recordsT) {
				intermediateBits.clear(bitIndex + 1, recordsMaxCapacity);
				break;
			}
		}
		return recordI;
	}
	
	private final void callBackRange(final int[] dataArray, final BitSetExposed intermediateBits, final int minValue, final int maxValue) {
		
		int recordsMaxCapacity = dataArray.length;

		long[] words = intermediateBits.toLongArrayExposed();
		int wordsT = words.length;

		int recordsT = intermediateBits.cardinality();
		int recordI = 0;
		
		int maxWordIndex = (recordsMaxCapacity / 64) + 1;
		for(int wordIndex = 0; wordIndex < maxWordIndex; wordIndex++) {
			if (wordIndex >= wordsT) break;
			long word = words[wordIndex];
			int bitStartIndex = wordIndex * 64;
			int bitEndIndex =  bitStartIndex + 64;
			if(bitEndIndex > recordsMaxCapacity) bitEndIndex = recordsMaxCapacity;

			recordI = operateRange(dataArray, intermediateBits, recordsT, recordI, word, bitStartIndex, bitEndIndex, recordsMaxCapacity, minValue, maxValue);
			if ( recordI >= recordsT) break;
		}
	} 
	
	private final int operateRange(final int[] dataArray, final BitSetExposed intermediateBits, final int recordsT, int recordI, final long word, 
			final int bitStartIndex, final int bitEndIndex, final int recordsMaxCapacity, final int minValue, final int maxValue) {
		
		for (int bitIndex = bitStartIndex; bitIndex < bitEndIndex; bitIndex++) {
			boolean bitVal = ((word & (1L << bitIndex)) != 0);
			if (bitVal) {
				int storedVal = dataArray[bitIndex];
				if(storedVal < minValue || storedVal > maxValue) {
					intermediateBits.clear(bitIndex);
				}
				recordI++;
			} 
			if ( recordI >= recordsT) {
				intermediateBits.clear(bitIndex + 1, recordsMaxCapacity);
				break;
			}
		}
		return recordI;
	}
	
	public final RecordIndexes opwork(final Operation operation, final int... values) {
		int length = availablePosBitSets.length;
		BitSetExposed[] intermediateBits = new BitSetExposed[length];
		for ( int i=0; i < length; i++) {
			intermediateBits[i] = this.bitsCache.take();
			BitSetExposed bs = availablePosBitSets[i];
			BitSetExposed nullStrip = nullStripArray[i];
			
			intermediateBits[i].or(bs);
			
			if(bs.nextSetBit(0) >= 0) {
				int[] dataArray = columnDataArray[i];
				switch (operation) {
				case GREATER_THAN:
					intermediateBits[i].andNot(nullStrip);
					callBackGreaterThan(dataArray, intermediateBits[i], values[0]);
					break;
					
				case GREATER_THAN_EQUAL_TO:
					intermediateBits[i].andNot(nullStrip);
					callBackGreaterThanEqualTo(dataArray, intermediateBits[i], values[0]);
					break;
				
				case LESSER_THAN:
					intermediateBits[i].andNot(nullStrip);
					callBackLesserThan(dataArray, intermediateBits[i], values[0]);
					break;
					
				case LESSER_THAN_EQUAL_TO:
					intermediateBits[i].andNot(nullStrip);
					callBackLesserThanEqualTo(dataArray, intermediateBits[i], values[0]);
					break;
					
				case EQUAL_TO:
					intermediateBits[i].andNot(nullStrip);
					if(values.length == 1) {
						callBackEqualTo(dataArray, intermediateBits[i], values[0]);
					} else {
						callBackEqualTo(dataArray, intermediateBits[i], values);
					}
					break;
					
				case RANGE:
					intermediateBits[i].andNot(nullStrip);
					callBackRange(dataArray, intermediateBits[i], values[0], values[1]);
					break;
					
				case MISSING:
					intermediateBits[i].and(nullStrip);
					break;
					
				case EXISTS:
					intermediateBits[i].andNot(nullStrip);
					break;

				default:
					break;
				}
			}
		}
		return new RecordIndexesImpl(intermediateBits, availablePosBitSets, bitsCache);
	}
	
	@Override
	public final RecordIndexes greaterThan(int value) {
		RoadRunnerUtils.validateCompareValues(fieldName, value);
		return opwork(Operation.GREATER_THAN, value);
	}

	@Override
	public final RecordIndexes lessThan(int value) {
		RoadRunnerUtils.validateCompareValues(fieldName, value);
		return opwork(Operation.LESSER_THAN, value);
	}

	@Override
	public final RecordIndexes greaterThanEqualTo(int value) {
		RoadRunnerUtils.validateCompareValues(fieldName, value);
		return opwork(Operation.GREATER_THAN_EQUAL_TO, value);
	}

	@Override
	public final RecordIndexes lessThanEqualTo(int value) {
		RoadRunnerUtils.validateCompareValues(fieldName, value);
		return opwork(Operation.LESSER_THAN_EQUAL_TO, value);
	}

	@Override
	public final RecordIndexes equalTo(int value) {
		RoadRunnerUtils.validateCompareValues(fieldName, value);
		return opwork(Operation.EQUAL_TO, value);
	}

	@Override
	public final RecordIndexes equalTo(int[] values) {
		RoadRunnerUtils.validateCompareValues(fieldName, values);
		return opwork(Operation.EQUAL_TO, values);
	}

	@Override
	public final RecordIndexes range(int minValue, int maxValue) {
		int[] values = new int[]{minValue, maxValue};
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

