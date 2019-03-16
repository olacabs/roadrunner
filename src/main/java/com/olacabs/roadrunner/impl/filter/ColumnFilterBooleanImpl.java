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
import com.olacabs.roadrunner.api.filter.ColumnFilterBoolean;
import com.olacabs.roadrunner.impl.BitsetCache;
import com.olacabs.roadrunner.impl.MapWrapper;
import com.olacabs.roadrunner.impl.PartitionBucket;
import com.olacabs.roadrunner.impl.RecordIndexesImpl;

public class ColumnFilterBooleanImpl implements ColumnFilterBoolean {

	String fieldName;
	MapWrapper[] invertedIndexArray;
	BitSetExposed[] availablePosBitSets;
	BitsetCache bitsCache;

	public  ColumnFilterBooleanImpl(String fieldName, MapWrapper[] invertedIndexArray, BitSetExposed[] availablePosBitSets, BitsetCache bitsCache) {
		this.fieldName = fieldName;
		this.invertedIndexArray = invertedIndexArray;
		this.availablePosBitSets = availablePosBitSets;
		this.bitsCache = bitsCache;
	}

	@Override
	public RecordIndexes isTrue() {
		int length = availablePosBitSets.length;
		BitSetExposed[] intermediateBits = new BitSetExposed[length];
		String key = fieldName + PartitionBucket.SEPARATOR + "true";
		for ( int i=0; i < length; i++) {
			intermediateBits[i] = this.bitsCache.take();
			if(availablePosBitSets[i].nextSetBit(0) >= 0) {
				BitSetExposed bitSet = invertedIndexArray[i].dataMap.get(key);
				if(bitSet != null) {
					intermediateBits[i].or(availablePosBitSets[i]);
					intermediateBits[i].and(bitSet);
				}
			}
		}
		return new RecordIndexesImpl(intermediateBits, availablePosBitSets, bitsCache);
	}

	@Override
	public RecordIndexes isFalse() {
		int length = availablePosBitSets.length;
		BitSetExposed[] intermediateBits = new BitSetExposed[length];
		String key = fieldName + PartitionBucket.SEPARATOR + "false";
		for ( int i=0; i < length; i++) {
			intermediateBits[i] = this.bitsCache.take();
			
			if(availablePosBitSets[i].nextSetBit(0) >= 0) {
				BitSetExposed bitSet = invertedIndexArray[i].dataMap.get(key);
				if(bitSet != null) {
					intermediateBits[i].or(availablePosBitSets[i]);
					intermediateBits[i].and(bitSet);
				}
			}
		}
		return new RecordIndexesImpl(intermediateBits, availablePosBitSets, bitsCache);
	}

	@Override
	public RecordIndexes isNull() {
		int length = availablePosBitSets.length;
		BitSetExposed[] intermediateBits = new BitSetExposed[length];
		String key = fieldName + PartitionBucket.SEPARATOR + "null";
		for ( int i=0; i < length; i++) {
			intermediateBits[i] = this.bitsCache.take();
			
			if(availablePosBitSets[i].nextSetBit(0) >= 0) {
				BitSetExposed bitSet = invertedIndexArray[i].dataMap.get(key);
				if(bitSet != null) {
					intermediateBits[i].or(availablePosBitSets[i]);
					intermediateBits[i].and(bitSet);
				}
			}
		}
		return new RecordIndexesImpl(intermediateBits, availablePosBitSets, bitsCache);
	}
}
