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
package com.olacabs.roadrunner.impl;

import com.olacabs.BitSetExposed;
import com.olacabs.roadrunner.api.RecordIndexes;

public final class RecordIndexesImpl implements RecordIndexes {
	
	private BitSetExposed[] intermediateBitSet;
	
	private BitSetExposed[] availableBitSet;
	
	private BitsetCache bitsetCache;

	public RecordIndexesImpl(BitSetExposed[] intermediateBitSet, BitSetExposed[] availableBitSet, BitsetCache bitsetCache) {
		this.intermediateBitSet = intermediateBitSet;
		this.availableBitSet = availableBitSet;
		this.bitsetCache = bitsetCache;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * com.ola.storage.api.RecordIndexes#or(com.ola.storage.api.RecordIndexes)
	 */
	@Override
	public final RecordIndexes or(final RecordIndexes recordIndexes) {

		int intermediateBitSetT = intermediateBitSet.length;
		for (int i = 0; i < intermediateBitSetT; i++) {
			RecordIndexesImpl inputRI = (RecordIndexesImpl) recordIndexes;
			this.intermediateBitSet[i].or(inputRI.intermediateBitSet[i]);
		}
		return this;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * com.ola.storage.api.RecordIndexes#and(com.ola.storage.api.RecordIndexes)
	 */
	@Override
	public final RecordIndexes and(final RecordIndexes recordIndexes) {
		int intermediateBitSetT = intermediateBitSet.length;
		for (int i = 0; i < intermediateBitSetT; i++) {
			RecordIndexesImpl inputRI = (RecordIndexesImpl) recordIndexes;
			this.intermediateBitSet[i].and(inputRI.intermediateBitSet[i]);
		}
		return this;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * com.ola.storage.api.RecordIndexes#not(com.ola.storage.api.RecordIndexes)
	 */
	@Override
	public final RecordIndexes not(final RecordIndexes recordIndexes) {
		int intermediateBitSetT = intermediateBitSet.length;
		for (int i = 0; i < intermediateBitSetT; i++) {
			RecordIndexesImpl inputRI = (RecordIndexesImpl) recordIndexes;
			BitSetExposed tempBitSet = bitsetCache.take();
			tempBitSet.or(inputRI.availableBitSet[i]);
			tempBitSet.andNot(inputRI.intermediateBitSet[i]);
			this.intermediateBitSet[i].and(tempBitSet);
		}
		return this;
	}

	@Override
	public final BitSetExposed[] getIds() {
		return intermediateBitSet;
	}

}
