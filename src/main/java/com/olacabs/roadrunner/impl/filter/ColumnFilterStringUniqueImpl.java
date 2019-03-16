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

import com.olacabs.BitSetExposed;
import com.olacabs.roadrunner.api.RecordIndexes;
import com.olacabs.roadrunner.api.filter.ColumnFilterString;
import com.olacabs.roadrunner.impl.BitsetCache;
import com.olacabs.roadrunner.impl.RecordIndexesImpl;
import com.olacabs.roadrunner.utils.RoadRunnerUtils;

public class ColumnFilterStringUniqueImpl implements ColumnFilterString {
	
	String fieldName;
	String[][] columnStringArray;
	BitSetExposed[] nullStripArray;
	BitSetExposed[] availablePosBitSets;
	BitsetCache bitsCache;
	
	public ColumnFilterStringUniqueImpl(String fieldName, String[][] columnStringArray, BitSetExposed[] nullStripArray, BitSetExposed[] availablePosBitSets, BitsetCache bitsCache) {
		super();
		this.fieldName = fieldName;
		this.columnStringArray = columnStringArray;
		this.nullStripArray = nullStripArray;
		this.availablePosBitSets = availablePosBitSets;
		this.bitsCache = bitsCache;
	}
	
	private final void callBackEquals(final String[] stringArray, final BitSetExposed intermediateBits, final String value) {
		for (int pos = intermediateBits.nextSetBit(0); pos >= 0; pos = intermediateBits.nextSetBit(pos+1)) {
			String storedVal = stringArray[pos];
			if(storedVal == null || ! storedVal.equals(value)) {
				intermediateBits.clear(pos);
			}
		}
	} 
	
	private final void callBackEqualsIgnoreCase(final String[] stringArray, final BitSetExposed intermediateBits, final String value) {
		for (int pos = intermediateBits.nextSetBit(0); pos >= 0; pos = intermediateBits.nextSetBit(pos+1)) {
			String storedVal = stringArray[pos];
			if(storedVal == null || ! storedVal.equalsIgnoreCase(value)) {
				intermediateBits.clear(pos);
			}
		}
	} 
	
	private final void callBackIn(final String[] stringArray, final BitSetExposed intermediateBits, final String[] values, final boolean ignoreCase) {
		int length = values.length;
		for (int pos = intermediateBits.nextSetBit(0); pos >= 0; pos = intermediateBits.nextSetBit(pos+1)) {
			String storedVal = stringArray[pos];
			boolean match = false;
			if(storedVal != null) {
				for(int i = 0; i < length; i++) {
					match = ignoreCase ? storedVal.equalsIgnoreCase(values[i]) : storedVal.equals(values[i]);
					if(match) break;
				}
			}
			if(! match) {
				intermediateBits.clear(pos);
			}
		}
	}
	
	private final void callBackNotEquals(final String[] stringArray, final BitSetExposed intermediateBits, final String value) {
		for (int pos = intermediateBits.nextSetBit(0); pos >= 0; pos = intermediateBits.nextSetBit(pos+1)) {
			String storedVal = stringArray[pos];
			if(storedVal != null && storedVal.equals(value)) {
				intermediateBits.clear(pos);
			}
		}
	} 
	
	private final void callBackNotEqualsIgnoreCase(final String[] stringArray, final BitSetExposed intermediateBits, final String value) {
		for (int pos = intermediateBits.nextSetBit(0); pos >= 0; pos = intermediateBits.nextSetBit(pos+1)) {
			String storedVal = stringArray[pos];
			if(storedVal != null && storedVal.equalsIgnoreCase(value)) {
				intermediateBits.clear(pos);
			}
		}
	} 
	
	private final void callBackNotIn(final String[] stringArray, final BitSetExposed intermediateBits, final String[] values, final boolean ignoreCase) {
		int length = values.length;
		for (int pos = intermediateBits.nextSetBit(0); pos >= 0; pos = intermediateBits.nextSetBit(pos+1)) {
			String storedVal = stringArray[pos];
			if(storedVal != null) {
				for(int i = 0; i < length; i++) {
					boolean equals = ignoreCase ? storedVal.equalsIgnoreCase(values[i]) : storedVal.equals(values[i]);
					if(equals) {				
						intermediateBits.clear(pos);
						break;
					}
				}
			}
		}
	}
	
	private final void callBackPrefix(final String[] stringArray, final BitSetExposed intermediateBits, final String value) {
		for (int pos = intermediateBits.nextSetBit(0); pos >= 0; pos = intermediateBits.nextSetBit(pos+1)) {
			String storedVal = stringArray[pos];
			if(storedVal == null || ! storedVal.startsWith(value)) {
				intermediateBits.clear(pos);
			}
		}
	}
	
	final public RecordIndexes opwork(final Operation operation, final String... values) {
		BitSetExposed[] intermediateBits = new BitSetExposed[availablePosBitSets.length];
		for ( int i=0; i < availablePosBitSets.length; i++) {

			intermediateBits[i] = this.bitsCache.take();
			BitSetExposed bs = availablePosBitSets[i];
			BitSetExposed nullStrip = nullStripArray[i];
			if(bs.nextSetBit(0) >= 0) {
				String[] stringArray = columnStringArray[i];
				switch (operation) {
				case EQUALS:
					intermediateBits[i].or(bs);
					intermediateBits[i].andNot(nullStrip);
					callBackEquals(stringArray, intermediateBits[i], values[0]);
					break;
					
				case EQUALS_IGNORE_CASE:
					intermediateBits[i].or(bs);
					intermediateBits[i].andNot(nullStrip);
					callBackEqualsIgnoreCase(stringArray, intermediateBits[i], values[0]);
					break;
				
				case IN:
					intermediateBits[i].or(bs);
					intermediateBits[i].andNot(nullStrip);
					callBackIn(stringArray, intermediateBits[i], values, false);
					break;
					
				case IN_IGNORE_CASE:
					intermediateBits[i].or(bs);
					intermediateBits[i].andNot(nullStrip);
					callBackIn(stringArray, intermediateBits[i], values, true);
					break;
					
				case NOT_EQUALS:
					intermediateBits[i].or(bs);
					callBackNotEquals(stringArray, intermediateBits[i], values[0]);
					break;
					
				case NOT_EQUALS_IGNORE_CASE:
					intermediateBits[i].or(bs);
					callBackNotEqualsIgnoreCase(stringArray, intermediateBits[i], values[0]);
					break;
					
				case NOT_IN:
					intermediateBits[i].or(bs);
					callBackNotIn(stringArray, intermediateBits[i], values, false);
					break;
					
				case NOT_IN_IGNORE_CASE:
					intermediateBits[i].or(bs);
					callBackNotIn(stringArray, intermediateBits[i], values, true);
					break;
					
				case MISSING:
					intermediateBits[i].or(bs);
					intermediateBits[i].and(nullStrip);
					break;
					
				case EXISTS:
					intermediateBits[i].or(bs);
					intermediateBits[i].andNot(nullStrip);
					break;
					
				case PREFIX:
					intermediateBits[i].or(bs);
					intermediateBits[i].andNot(nullStrip);
					callBackPrefix(stringArray, intermediateBits[i], values[0]);
					break;

				default:
					break;
				}
			}
		}
		return new RecordIndexesImpl(intermediateBits, availablePosBitSets, bitsCache);
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
		return opwork(Operation.PREFIX, value);
	}

}
