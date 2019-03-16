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

import java.util.HashSet;
import java.util.Set;

import com.olacabs.BitSetExposed;
import com.olacabs.roadrunner.api.RecordIndexes;
import com.olacabs.roadrunner.api.filter.ColumnFilterString;
import com.olacabs.roadrunner.api.filter.DictionaryString;
import com.olacabs.roadrunner.impl.BitsetCache;
import com.olacabs.roadrunner.impl.MapWrapper;
import com.olacabs.roadrunner.impl.PartitionBucket;
import com.olacabs.roadrunner.impl.RecordIndexesImpl;
import com.olacabs.roadrunner.utils.RoadRunnerUtils;

public class ColumnFilterStringRepeatedImpl implements ColumnFilterString {

	String fieldName;
	MapWrapper[] invertedIndexArray;
	DictionaryString dictionary;
	BitSetExposed[] availablePosBitSets;
	BitsetCache bitsCache;
	
	public ColumnFilterStringRepeatedImpl(String fieldName, MapWrapper[] invertedIndexArray, DictionaryString dictionary, 
			BitSetExposed[] availablePosBitSets, BitsetCache bitsCache) {
		super();
		this.fieldName = fieldName; 
		this.invertedIndexArray = invertedIndexArray;
		this.dictionary = dictionary;
		this.availablePosBitSets = availablePosBitSets;
		this.bitsCache = bitsCache;
	}

	@Override
	public RecordIndexes equals(String value) {
		RoadRunnerUtils.validateCompareValues(fieldName, value);

		int availablePosBitSetsT = availablePosBitSets.length;
		BitSetExposed[] intermediateBits = new BitSetExposed[availablePosBitSetsT];
		String fieldNameWithValue = fieldName + PartitionBucket.SEPARATOR + String.valueOf(value);

		for (int i = 0; i < availablePosBitSetsT; i++) {

			intermediateBits[i] = this.bitsCache.take();
			BitSetExposed availablePosBitSet = availablePosBitSets[i];
			if(availablePosBitSet.nextSetBit(0) >= 0) {
				BitSetExposed bs = invertedIndexArray[i].dataMap.get(fieldNameWithValue);

				if(bs != null) {
					intermediateBits[i].or(availablePosBitSet);
					intermediateBits[i].and(bs);
				}
			}

		}
		return new RecordIndexesImpl(intermediateBits, availablePosBitSets, bitsCache);
	}

	@Override
	public RecordIndexes equalsIgnoreCase(String value) {
		RoadRunnerUtils.validateCompareValues(fieldName, value);

		String[] values = dictionary.keys();
		int availablePosBitSetsT = availablePosBitSets.length;
		BitSetExposed[] intermediateBits = new BitSetExposed[availablePosBitSetsT];

		for (int i = 0; i < availablePosBitSetsT; i++) {

			intermediateBits[i] = this.bitsCache.take();
			BitSetExposed availablePosBitSet = availablePosBitSets[i];

			if(availablePosBitSet.nextSetBit(0) >= 0) {

				intermediateBits[i].or(availablePosBitSet);
				BitSetExposed bs = this.bitsCache.take();

				int valuesT = values.length;

				for(int pos = 0; pos < valuesT; pos++) {

					String val = String.valueOf(values[pos]);
					if(val.equalsIgnoreCase(value)) {
						String fieldNameWithValue = fieldName + PartitionBucket.SEPARATOR + val;
						BitSetExposed valBitSet = invertedIndexArray[i].dataMap.get(fieldNameWithValue);
						if(valBitSet != null) {
							bs.or(valBitSet);
						} 
					}

				}

				intermediateBits[i].and(bs);
			}

		}
		return new RecordIndexesImpl(intermediateBits, availablePosBitSets, bitsCache);
	}
	
	@Override
	public RecordIndexes in(String[] values) {
		return in(values, false);
	}

	@Override
	public RecordIndexes in(String[] values, boolean ignoreCase) {
		RoadRunnerUtils.validateCompareValues(fieldName, values);
		if(ignoreCase) {
			for(int i = 0; i < values.length; i++) {
				values[i] = values[i] == null ? null : values[i].toLowerCase();
			}
		}
		Set<String> keysToCheck = new HashSet<>();
		String[] dictValuesArray = dictionary.lowercaseKeys();
		String[] dictKeysArray = dictionary.keys();
		int dictValuesArrayT = dictValuesArray.length;
		String fileNameWithPartitioner = fieldName + PartitionBucket.SEPARATOR;
		
		for(int i = 0; i < dictValuesArrayT; i++) {
			String dictValue = dictKeysArray[i];
			if(ignoreCase) {
				dictValue = dictValuesArray[i];
			}
			boolean match = false;
			for(String valueToCompare : values) {
				match = dictValue.equals(valueToCompare);
				if(match) break;
			}
			if(match) {
				String fieldNameWithValue = fileNameWithPartitioner + String.valueOf(dictKeysArray[i]);
				keysToCheck.add(fieldNameWithValue);
			}
		}

		int availablePosBitSetsT = availablePosBitSets.length;
		BitSetExposed[] intermediateBits = new BitSetExposed[availablePosBitSetsT];

		for (int i = 0; i < availablePosBitSetsT; i++) {

			intermediateBits[i] = this.bitsCache.take();
			if(availablePosBitSets[i].nextSetBit(0) >= 0) {
				intermediateBits[i].or(availablePosBitSets[i]);

				BitSetExposed bs = this.bitsCache.take();
				for(String key : keysToCheck) {
					BitSetExposed valBitSet = invertedIndexArray[i].dataMap.get(key);
					if(valBitSet != null) {
						bs.or(valBitSet);
					} 
				}
				intermediateBits[i].and(bs);
			}
		}
		return new RecordIndexesImpl(intermediateBits, availablePosBitSets, bitsCache);
	}

	@Override
	public RecordIndexes notEquals(String value) {
		RoadRunnerUtils.validateCompareValues(fieldName, value);

		String[] values = dictionary.keys();
		int availablePosBitSetsT = availablePosBitSets.length;
		BitSetExposed[] intermediateBits = new BitSetExposed[availablePosBitSetsT];
		
		for (int i = 0; i < availablePosBitSetsT; i++) {

			intermediateBits[i] = this.bitsCache.take();

			if(availablePosBitSets[i].nextSetBit(0) >= 0) {

				intermediateBits[i].or(availablePosBitSets[i]);
				BitSetExposed bs = this.bitsCache.take();
				int valuesT = values.length;

				for(int pos = 0; pos < valuesT; pos++) {
					String val = String.valueOf(values[pos]);
					if(! val.equals(value)) {
						String fieldNameWithValue = fieldName + PartitionBucket.SEPARATOR + val;
						BitSetExposed valBitSet = invertedIndexArray[i].dataMap.get(fieldNameWithValue);
						if(valBitSet != null) {
							bs.or(valBitSet);
						} 
					}
				}

				intermediateBits[i].and(bs);
			}
		}
		return new RecordIndexesImpl(intermediateBits, availablePosBitSets, bitsCache);
	}

	@Override
	public RecordIndexes notEqualsIgnoreCase(String value) {
		RoadRunnerUtils.validateCompareValues(fieldName, value);

		String[] values = dictionary.keys();
		int availablePosBitSetsT = availablePosBitSets.length;
		BitSetExposed[] intermediateBits = new BitSetExposed[availablePosBitSetsT];
		
		for (int i = 0; i < availablePosBitSetsT; i++) {

			intermediateBits[i] = this.bitsCache.take();

			if(availablePosBitSets[i].nextSetBit(0) >= 0) {

				intermediateBits[i].or(availablePosBitSets[i]);
				BitSetExposed bs = this.bitsCache.take();
				int valuesT = values.length;

				for(int pos = 0; pos < valuesT; pos++) {
					String val = String.valueOf(values[pos]);
					if(! val.equalsIgnoreCase(value)) {
						String fieldNameWithValue = fieldName + PartitionBucket.SEPARATOR + val;
						BitSetExposed valBitSet = invertedIndexArray[i].dataMap.get(fieldNameWithValue);
						if(valBitSet != null) {
							bs.or(valBitSet);
						} 
					}
				}
				intermediateBits[i].and(bs);
			}
		}
		return new RecordIndexesImpl(intermediateBits, availablePosBitSets, bitsCache);
	}
	
	@Override
	public RecordIndexes notIn(String[] values) {
		return notIn(values, false);
	}

	@Override
	public RecordIndexes notIn(String[] values, boolean ignoreCase) {
		RoadRunnerUtils.validateCompareValues(fieldName, values);
		if(ignoreCase) {
			for(int i = 0; i < values.length; i++) {
				values[i] = values[i] == null ? null : values[i].toLowerCase();
			}
		}

		Set<String> keysToCheck = new HashSet<>();
		String[] dictValuesArray = dictionary.lowercaseKeys();
		String[] dictKeysArray = dictionary.keys();
		int dictValuesArrayT = dictValuesArray.length;
		String fileNameWithPartitioner = fieldName + PartitionBucket.SEPARATOR;

		for(int i = 0; i < dictValuesArrayT; i++) {
			String dictValue = dictKeysArray[i];
			if(ignoreCase) {
				dictValue = dictValuesArray[i];
			}
			boolean match = false;
			for(String valueToCompare : values) {
				match = dictValue.equals(valueToCompare);
				if(match) break;
			}
			if(! match) {
				String fieldNameWithValue = fileNameWithPartitioner + String.valueOf(dictKeysArray[i]);
				keysToCheck.add(fieldNameWithValue);
			}
		}

		int availablePosBitSetsT = availablePosBitSets.length;
		BitSetExposed[] intermediateBits = new BitSetExposed[availablePosBitSetsT];

		for (int i = 0; i < availablePosBitSetsT; i++) {

			intermediateBits[i] = this.bitsCache.take();
			if(availablePosBitSets[i].nextSetBit(0) >= 0) {
				intermediateBits[i].or(availablePosBitSets[i]);

				BitSetExposed bs = this.bitsCache.take();
				for(String key : keysToCheck) {
					BitSetExposed valBitSet = invertedIndexArray[i].dataMap.get(key);
					if(valBitSet != null) {
						bs.or(valBitSet);
					} 
				}
				intermediateBits[i].and(bs);
			}
		}
		return new RecordIndexesImpl(intermediateBits, availablePosBitSets, bitsCache);
	}

	@Override
	public RecordIndexes missing() {

		String[] values = dictionary.keys();
		int availablePosBitSetsT = availablePosBitSets.length;
		BitSetExposed[] intermediateBits = new BitSetExposed[availablePosBitSetsT];
		
		for (int i = 0; i < availablePosBitSetsT; i++) {

			intermediateBits[i] = this.bitsCache.take();
			if(availablePosBitSets[i].nextSetBit(0) >= 0) {
				intermediateBits[i].or(availablePosBitSets[i]);

				BitSetExposed bs = this.bitsCache.take();
				int valuesT = values.length;
				for(int pos = 0; pos < valuesT; pos++) {
					String key = fieldName + PartitionBucket.SEPARATOR + String.valueOf(values[pos]);
					BitSetExposed valBitSet = invertedIndexArray[i].dataMap.get(key);
					if(valBitSet != null) {
						bs.or(valBitSet);
					} 
				}
				intermediateBits[i].andNot(bs);
			}
		}
		return new RecordIndexesImpl(intermediateBits, availablePosBitSets, bitsCache);
	}

	@Override
	public RecordIndexes exists() {

		String[] values = dictionary.keys();
		int availablePosBitSetsT = availablePosBitSets.length;
		BitSetExposed[] intermediateBits = new BitSetExposed[availablePosBitSetsT];

		for (int i = 0; i < availablePosBitSetsT; i++) {

			intermediateBits[i] = this.bitsCache.take();
			if(availablePosBitSets[i].nextSetBit(0) >= 0) {

				intermediateBits[i].or(availablePosBitSets[i]);
				BitSetExposed bs = this.bitsCache.take();

				int valuesT = values.length;
				for(int pos = 0; pos < valuesT; pos++) {
					String key = fieldName + PartitionBucket.SEPARATOR + String.valueOf(values[pos]);
					BitSetExposed valBitSet = invertedIndexArray[i].dataMap.get(key);
					if(valBitSet != null) {
						bs.or(valBitSet);
					} 
				}
				intermediateBits[i].and(bs);
			}
		}
		return new RecordIndexesImpl(intermediateBits, availablePosBitSets, bitsCache);
	}

	@Override
	public RecordIndexes prefix(String value) {
		RoadRunnerUtils.validateCompareValues(fieldName, value);
		
		String[] values = dictionary.keys();
		int availablePosBitSetsT = availablePosBitSets.length;
		BitSetExposed[] intermediateBits = new BitSetExposed[availablePosBitSetsT];
		
		for (int i = 0; i < availablePosBitSetsT; i++) {

			intermediateBits[i] = this.bitsCache.take();

			if(availablePosBitSets[i].nextSetBit(0) >= 0) {

				intermediateBits[i].or(availablePosBitSets[i]);
				BitSetExposed bs = this.bitsCache.take();
				int valuesT = values.length;

				for(int pos = 0; pos < valuesT; pos++) {
					String val = String.valueOf(values[pos]);
					if(val.startsWith(value)) {
						String fieldNameWithVal = fieldName + PartitionBucket.SEPARATOR + val;
						BitSetExposed valBitSet = invertedIndexArray[i].dataMap.get(fieldNameWithVal);
						if(valBitSet != null) {
							bs.or(valBitSet);
						}
					}
				}

				intermediateBits[i].and(bs);
			}
		}
		return new RecordIndexesImpl(intermediateBits, availablePosBitSets, bitsCache);
	}
}
