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

import java.util.ArrayList;
import java.util.List;

import com.olacabs.BitSetExposed;
import com.olacabs.roadrunner.api.filter.ColumnFilterInteger;
import com.olacabs.roadrunner.api.filter.DictionaryInteger;
import com.olacabs.roadrunner.api.RecordIndexes;

import com.olacabs.roadrunner.impl.BitsetCache;
import com.olacabs.roadrunner.impl.RecordIndexesImpl;
import com.olacabs.roadrunner.impl.MapWrapper;
import com.olacabs.roadrunner.impl.PartitionBucket;
import com.olacabs.roadrunner.utils.RoadRunnerUtils;

/**
 * Don't Edit this code, it is generated using templates
 */
public class ColumnFilterIntegerRepeatedImpl implements ColumnFilterInteger {

	String fieldName;
	MapWrapper[] invertedIndexArray;
	DictionaryInteger dictionary;

	BitSetExposed[] availablePosBitSets;
	BitsetCache bitsCache;

	public ColumnFilterIntegerRepeatedImpl(String fieldName, MapWrapper[] invertedIndexArray,
			DictionaryInteger dictionary, BitSetExposed[] availablePosBitSets, BitsetCache bitsCache) {
		super();
		this.fieldName = fieldName;
		this.invertedIndexArray = invertedIndexArray;
		this.dictionary = dictionary;
		this.availablePosBitSets = availablePosBitSets;
		this.bitsCache = bitsCache;
	}

	private final int[] getKeysForEqualToOp(final int value) {
		if(dictionary.contains(value)) {
			return new int[]{value};
		}
		return new int[]{};
	} 

	private final int[] getKeysForEqualToOp(final int[] values) {
		List<Integer> dataList = new ArrayList<>();
		for(int i = 0; i < values.length; i++) {
			if(dictionary.contains(values[i])) {
				dataList.add(values[i]);
			}
		}
		if(dataList.isEmpty()) {
			return new int[]{};
		}

		int size = dataList.size();
		int[] keys = new int[size];
		for(int i = 0; i < size; i++ ) {
			keys[i] = dataList.get(i);
		}
		return keys;
	} 

	public final RecordIndexes opwork(final Operation operation, final int... values) {
		int[] valuesToCheck;
		switch (operation) {
		case GREATER_THAN:
			valuesToCheck = dictionary.greaterThan(values[0]);
			break;

		case GREATER_THAN_EQUAL_TO:
			valuesToCheck = dictionary.greaterThanEqualTo(values[0]); 
			break;

		case LESSER_THAN:
			valuesToCheck = dictionary.lessThan(values[0]);
			break;

		case LESSER_THAN_EQUAL_TO:
			valuesToCheck = dictionary.lessThanEqualTo(values[0]);
			break;

		case EQUAL_TO:
			if(values.length == 1) {
				valuesToCheck = getKeysForEqualToOp(values[0]);
			} else {
				valuesToCheck = getKeysForEqualToOp(values);
			}
			break;

		case RANGE:
			valuesToCheck = dictionary.rangeValues(values[0], values[1]);
			break;
			
		case MISSING:
			valuesToCheck = dictionary.keys();
			break;
		
		case EXISTS:
			valuesToCheck = dictionary.keys();
			break;

		default:
			valuesToCheck = new int[]{};
			break;
		}
		
		int length = availablePosBitSets.length;
		int valuesLength = valuesToCheck.length;
		String keyPrefix = fieldName + PartitionBucket.SEPARATOR;
		
		BitSetExposed[] intermediateBits = new BitSetExposed[length];
		BitSetExposed valBitSet = this.bitsCache.take();
		
		for ( int i=0; i < length; i++) {
			intermediateBits[i] = this.bitsCache.take();
			BitSetExposed bs = availablePosBitSets[i];

			if(bs.nextSetBit(0) >= 0) {
				intermediateBits[i].or(bs);
				for(int pos = 0; pos < valuesLength; pos++) {
					String key = keyPrefix + String.valueOf(valuesToCheck[pos]);
					BitSetExposed bitSet = invertedIndexArray[i].dataMap.get(key);
					if(bitSet != null) valBitSet.or(bitSet);
				}
				if(operation == Operation.MISSING) {
					intermediateBits[i].andNot(valBitSet);
				} else {
					intermediateBits[i].and(valBitSet);
				}
			}
			valBitSet.clear();	
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

