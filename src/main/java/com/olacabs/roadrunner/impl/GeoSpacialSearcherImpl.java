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

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Consumer;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;

import com.olacabs.BitSetExposed;
import com.olacabs.roadrunner.api.GeoSpatialRecord;
import com.olacabs.roadrunner.api.GeoSpatialSearcher;
import com.olacabs.roadrunner.api.IndexableField;
import com.olacabs.roadrunner.api.Record;
import com.olacabs.roadrunner.api.RecordIndexes;
import com.olacabs.roadrunner.api.Records;
import com.olacabs.roadrunner.api.filter.ColumnFilterBoolean;
import com.olacabs.roadrunner.api.filter.ColumnFilterDouble;
import com.olacabs.roadrunner.api.filter.ColumnFilterFloat;
import com.olacabs.roadrunner.api.filter.ColumnFilterInteger;
import com.olacabs.roadrunner.api.filter.ColumnFilterLong;
import com.olacabs.roadrunner.api.filter.ColumnFilterShort;
import com.olacabs.roadrunner.api.filter.ColumnFilterString;
import com.olacabs.roadrunner.impl.filter.ColumnFilterBooleanImpl;
import com.olacabs.roadrunner.impl.filter.ColumnFilterBooleanNonIndexedImpl;
import com.olacabs.roadrunner.impl.filter.ColumnFilterDoubleNonIndexedImpl;
import com.olacabs.roadrunner.impl.filter.ColumnFilterDoubleRepeatedImpl;
import com.olacabs.roadrunner.impl.filter.ColumnFilterDoubleUniqueImpl;
import com.olacabs.roadrunner.impl.filter.ColumnFilterFloatNonIndexedImpl;
import com.olacabs.roadrunner.impl.filter.ColumnFilterFloatRepeatedImpl;
import com.olacabs.roadrunner.impl.filter.ColumnFilterFloatUniqueImpl;
import com.olacabs.roadrunner.impl.filter.ColumnFilterIntegerNonIndexedImpl;
import com.olacabs.roadrunner.impl.filter.ColumnFilterIntegerRepeatedImpl;
import com.olacabs.roadrunner.impl.filter.ColumnFilterIntegerUniqueImpl;
import com.olacabs.roadrunner.impl.filter.ColumnFilterLongNonIndexedImpl;
import com.olacabs.roadrunner.impl.filter.ColumnFilterLongRepeatedImpl;
import com.olacabs.roadrunner.impl.filter.ColumnFilterLongUniqueImpl;
import com.olacabs.roadrunner.impl.filter.ColumnFilterShortNonIndexedImpl;
import com.olacabs.roadrunner.impl.filter.ColumnFilterShortRepeatedImpl;
import com.olacabs.roadrunner.impl.filter.ColumnFilterShortUniqueImpl;
import com.olacabs.roadrunner.impl.filter.ColumnFilterStringNonIndexedImpl;
import com.olacabs.roadrunner.impl.filter.ColumnFilterStringRepeatedImpl;
import com.olacabs.roadrunner.impl.filter.ColumnFilterStringUniqueImpl;
import com.olacabs.roadrunner.monitor.RoadRunnerMetricFactory;
import com.olacabs.roadrunner.utils.Haversine;
import com.olacabs.roadrunner.utils.LoggerFactory;
import com.olacabs.roadrunner.utils.RoadRunnerUtils;

class GeoSpacialSearcherImpl implements GeoSpatialSearcher {

	public static final class RecordCounter {
		int includedRecords = 0;
		int excludedRecords = 0;

		public void clear() {
			this.includedRecords = 0;
			this.excludedRecords = 0;
		}
	}

	private static RoadRunnerMetricFactory metrics = RoadRunnerMetricFactory.getInstance();

	private double latitude;

	private double longitude;

	private int radiusInMeters;
	
	private PartitionBucket[] partitionBuckets;
	
	private boolean useIndexFilter = true;

	Map<String, IndexableField<?>> columns;

	BitSetExposed[] resultBitset;

	BitsetCache bitsetCache = new BitsetCache();

	GeoSpatialRecordCache geospatialRecordCache = new GeoSpatialRecordCache();

	DictionaryHolder dictionaryHolder;

	GeoSpatialStoreImpl geoSpatialStoreImpl;

	RecordCounter recordCount = new RecordCounter();

	private static final Logger logger = LoggerFactory.getLogger(GeoSpacialSearcherImpl.class);
	
	public static final ThreadLocal<Integer> tempPartitionBucketId =
			new ThreadLocal<Integer>() {
		@Override protected Integer initialValue() {
			return 0;
		}
	};
	
	public String getNextId() {
		int intValue = tempPartitionBucketId.get().intValue();
		if(intValue == Integer.MAX_VALUE) {
			tempPartitionBucketId.set(0);
		} else {
			tempPartitionBucketId.set(intValue + 1);
		}
		return Thread.currentThread().getId() + "_" + intValue;
	}

	protected GeoSpacialSearcherImpl() {}

	protected GeoSpacialSearcherImpl setGeoSpatialStore(GeoSpatialStoreImpl geoSpatialStoreImpl) {
		this.geoSpatialStoreImpl = geoSpatialStoreImpl;
		return this;
	}

	public BitSetExposed[] getResultDocIds() {
		metrics.increment("searcher_getResultDocIds" , 1);
		return resultBitset;
	}

	public void setResultDocIds(BitSetExposed[] resultBitset) {
		metrics.increment("searcher_setResultDocIds" , 1);
		this.resultBitset = resultBitset;
	}

	private void setBuckets(PartitionBucket[] partitionBuckets, Map<String, IndexableField<?>> columns,
						   DictionaryHolder dictionaryHolder) {

		this.useIndexFilter = true;
		this.partitionBuckets = partitionBuckets;
		this.columns = columns;
		this.dictionaryHolder = dictionaryHolder;
		this.resultBitset = new BitSetExposed[partitionBuckets.length];
		for(int i = 0; i < partitionBuckets.length; i++) {
			PartitionBucket bucket = partitionBuckets[i];
			BitSetExposed resultBitSet = bitsetCache.take();
			BitSetExposed availableBits = bucket.getAvailableBits();
			resultBitSet.clear();
			resultBitSet.or(availableBits);
			resultBitset[i] = resultBitSet;
		}
	}

	public GeoSpatialSearcher setRadial(double latitude, double longitude, int radiusInMeters) {
		this.latitude = latitude;
		this.longitude = longitude;
		this.radiusInMeters = radiusInMeters;
		return this;
	}

	public GeoSpatialSearcher build() {
		if(geoSpatialStoreImpl == null) {
			return build(null);
		}
		Set<String> buckets = this.geoSpatialStoreImpl.partitionBuckets.keySet();
		String[] partitionValues = new String[buckets.size()];
		int counter = 0;
		for (String bucket: buckets) {
			partitionValues[counter] = bucket;
			counter++;
		}
		return build(partitionValues);
	}

    public GeoSpatialSearcher build(String[] partitionValues) {

		if(geoSpatialStoreImpl == null) {
			logger.error("Please initialize geoSpecialStore");
			throw new IllegalAccessError("Please initialize geoSpecialStore");
		}
		
		long start = System.nanoTime();
		boolean success = false;
		try {

			int partitionBucketsTotal = partitionValues.length;
			int bucketCount = 0;
			PartitionBucket[] notnullStrips = new PartitionBucket[partitionBucketsTotal];

			for(int i = 0; i < partitionBucketsTotal; i++) {
				notnullStrips[i] = geoSpatialStoreImpl.partitionBuckets.get(partitionValues[i]);
				if ( notnullStrips[i] != null ) bucketCount++;
			}

			PartitionBucket[] filteredPartitionBuckets = new PartitionBucket[bucketCount];
			bucketCount = 0;
			for(int i = 0; i < partitionBucketsTotal; i++) {
				if ( notnullStrips[i] != null ) {
					filteredPartitionBuckets[bucketCount] = notnullStrips[i];
					bucketCount++;
				}
			}
			this.setBuckets(filteredPartitionBuckets,
					geoSpatialStoreImpl.columns, geoSpatialStoreImpl.dictionaryHolder);
			success = true;
			return this;
		} finally {
			metrics.measureMethodCall("searcher_radial", (System.nanoTime()-start), success);
		}
	}
    
    @Override
	public GeoSpatialSearcher buildByIds(String[] ids) {
    		this.useIndexFilter = false;
    		String tempPartitionBucketId = String.valueOf(getNextId());
    		int length = ids.length;
		List<RecordHolder> recordHolderList = new ArrayList<>();
    		
		for(int i = 0; i < length; i++) {
			RecordHolder holder = geoSpatialStoreImpl.getRecordHolder(ids[i]);
    			
			if(holder != null && holder.getNewRecord() != null) {
	    			RecordHolder tempHolder = new RecordHolder();
	    			tempHolder.setNewRecord((RecordImplVersion) holder.getNewRecord().clone());
	    			recordHolderList.add(tempHolder); 
    			}
    		}
    		
		int size = recordHolderList.size();
		for(int i = 0; i < size; i++) {
			RecordHolder holder = recordHolderList.get(i);
			RecordImplVersion record = (RecordImplVersion) holder.getNewRecord();
			
			record.setPos(i);
			record.setPartitionValue(tempPartitionBucketId);
		}
		PartitionBucket[] partitionBuckets = new PartitionBucket[1];
		partitionBuckets[0] = new PartitionBucket(tempPartitionBucketId, recordHolderList.toArray(new RecordHolder[size]));
    		this.partitionBuckets = partitionBuckets;
    		this.columns = geoSpatialStoreImpl.columns;
    		this.resultBitset = new BitSetExposed[1];
    		resultBitset[0] = new BitSetExposed(size);
    		resultBitset[0].or(partitionBuckets[0].getAvailableBits());
    		return this;
    }

	/* (non-Javadoc)
	 * @see com.ola.storage.api.GeoSpatialSearcher#whereBoolean(java.lang.String)
	 */
	@Override
	public ColumnFilterBoolean whereBoolean(String columnName) {

		if(StringUtils.isEmpty(columnName)) {
			metrics.increment("searcher_whereBoolean_invalid_columnName", 1);
			logger.error("columnName is null / empty in whereBoolean search: {}", columnName);
			throw new IllegalArgumentException("columnName is invalid");
		}

		long start = System.nanoTime();
		try {
			IndexableField fld = columns.get(columnName);
			int length = partitionBuckets.length;

			//Not-Indexed Field processing
			if(fld == null || ! useIndexFilter) {

				metrics.increment(RoadRunnerUtils.format("searcher_whereBoolean_non_index_access_" + columnName), 1);
				RecordHolder[][] recordHolders = new RecordHolder[length][];
				String[] partitionBucketIds = new String[length];
				for(int i=0; i < length; i++) {
					PartitionBucket bucket = partitionBuckets[i];
					recordHolders[i] = bucket.getRecordHolders();
					partitionBucketIds[i] = bucket.id;
				}
				return new ColumnFilterBooleanNonIndexedImpl(partitionBucketIds, columnName, recordHolders, resultBitset, this.bitsetCache);
			}
			//			Indexed Field processing
			else {
				MapWrapper[] invertedIndexMapper = new MapWrapper[length];
				for (int i = 0; i < length; i++) {
					PartitionBucket bucket = partitionBuckets[i];
					invertedIndexMapper[i] = new MapWrapper(bucket.getInvertedIndex());
				}

				return new ColumnFilterBooleanImpl(columnName, invertedIndexMapper, resultBitset, bitsetCache);
			}
		} finally {
			metrics.measureMethodCall(RoadRunnerUtils.format("searcher_" + columnName), (System.nanoTime()-start), true);
		}
	}

	@Override
	public ColumnFilterShort whereShort(String columnName) {
		if(StringUtils.isEmpty(columnName)) {
			metrics.increment("searcher_whereShort_invalid_columnName", 1);
			logger.error("columnName is null / empty in whereShort search: {}", columnName);
			throw new IllegalArgumentException("columnName is invalid");
		}
		long start = System.nanoTime();
		try {
			IndexableField fld = columns.get(columnName);
			int length = partitionBuckets.length;
			if(fld == null || ! useIndexFilter) {
				metrics.increment(RoadRunnerUtils.format("searcher_whereShort_non_index_access_" + columnName), 1);
				RecordHolder[][] recordHolders = new RecordHolder[length][];
				String[] partitionBucketIds = new String[length];
				for(int i=0; i < length; i++) {
					PartitionBucket bucket = partitionBuckets[i];
					recordHolders[i] = bucket.getRecordHolders();
					partitionBucketIds[i] = bucket.id;
				}
				return new ColumnFilterShortNonIndexedImpl(partitionBucketIds, columnName, recordHolders, resultBitset, this.bitsetCache);
			} else {
				if (fld.isMostlyUnique()) {
					short[][] columnShortArray = new short[length][];
					BitSetExposed[] nullStripArray = new BitSetExposed[length];
					for(int i=0; i < length; i++) {
						PartitionBucket bucket = partitionBuckets[i];
						columnShortArray[i] = bucket.shortArrayIndex.get(columnName);
						nullStripArray[i] = bucket.nullStrips.get(columnName);
					}
					return new ColumnFilterShortUniqueImpl(columnName, columnShortArray, nullStripArray, resultBitset, bitsetCache);
				} else {
					MapWrapper[] invertedIndexMapper = new MapWrapper[length];
					for(int i=0; i < length; i++) {
						PartitionBucket bucket = partitionBuckets[i];
						invertedIndexMapper[i] = new MapWrapper(bucket.getInvertedIndex());
					}
					return new ColumnFilterShortRepeatedImpl(columnName, invertedIndexMapper, dictionaryHolder.shortDictionary.get(columnName), resultBitset, bitsetCache);
				}
			}
		} finally {
			metrics.measureMethodCall(RoadRunnerUtils.format("searcher_" + columnName), (System.nanoTime()-start), true);
		}
	}

	@Override
	public ColumnFilterInteger whereInt(String columnName) {

		if(StringUtils.isEmpty(columnName)) {
			metrics.increment("searcher_whereInt_invalid_columnName", 1);
			logger.error("columnName is null / empty in whereInt search: {}", columnName);
			throw new IllegalArgumentException("columnName is invalid");
		}

		long start = System.nanoTime();
		try {

			IndexableField fld = columns.get(columnName);
			int length = partitionBuckets.length;
			if(fld == null || ! useIndexFilter) {

				metrics.increment(RoadRunnerUtils.format("searcher_whereInt_non_index_access_" + columnName), 1);
				RecordHolder[][] recordHolders = new RecordHolder[length][];
				String[] partitionBucketIds = new String[length];
				for(int i=0; i < length; i++) {
					PartitionBucket bucket = partitionBuckets[i];
					recordHolders[i] = bucket.getRecordHolders();
					partitionBucketIds[i] = bucket.id;
				}
				return new ColumnFilterIntegerNonIndexedImpl(partitionBucketIds, columnName, recordHolders, resultBitset, this.bitsetCache);

			} else {
				if (fld.isMostlyUnique()) {
					int[][] columnIntArray = new int[length][];
					BitSetExposed[] nullStripArray = new BitSetExposed[length];
					for(int i=0; i < length; i++) {
						PartitionBucket bucket = partitionBuckets[i];
						columnIntArray[i] = bucket.intArrayIndex.get(columnName);
						nullStripArray[i] = bucket.nullStrips.get(columnName);
					}
					return new ColumnFilterIntegerUniqueImpl(columnName, columnIntArray, nullStripArray, resultBitset, bitsetCache);
				} else {
					MapWrapper[] invertedIndexMapper = new MapWrapper[length];
					for(int i=0; i < length; i++) {
						PartitionBucket bucket = partitionBuckets[i];
						invertedIndexMapper[i] = new MapWrapper(bucket.getInvertedIndex());
					}
					return new ColumnFilterIntegerRepeatedImpl(columnName, invertedIndexMapper, dictionaryHolder.intDictionary.get(columnName), resultBitset, bitsetCache);
				}
			}
		} finally {
			metrics.measureMethodCall(RoadRunnerUtils.format("searcher_" + columnName), (System.nanoTime()-start), true);
		}
	}

	@Override
	public ColumnFilterFloat whereFloat(String columnName) {

		if(StringUtils.isEmpty(columnName)) {
			metrics.increment("searcher_whereFloat_invalid_columnName", 1);
			logger.error("columnName is null / empty in whereFloat search: {}", columnName);
			throw new IllegalArgumentException("columnName is invalid");
		}

		long start = System.nanoTime();
		try {

			IndexableField fld = columns.get(columnName);
			int length = partitionBuckets.length;
			if(fld == null || ! useIndexFilter) {

				metrics.increment(RoadRunnerUtils.format("searcher_whereFloat_non_index_access_" + columnName), 1);
				RecordHolder[][] recordHolders = new RecordHolder[length][];
				String[] partitionBucketIds = new String[length];
				for(int i=0; i < length; i++) {
					PartitionBucket bucket = partitionBuckets[i];
					recordHolders[i] = bucket.getRecordHolders();
					partitionBucketIds[i] = bucket.id;
				}
				return new ColumnFilterFloatNonIndexedImpl(partitionBucketIds, columnName, recordHolders, resultBitset, this.bitsetCache);

			} else {
				if (fld.isMostlyUnique()) {
					float[][] columnFloatArray = new float[length][];
					BitSetExposed[] nullStripArray = new BitSetExposed[length];
					for(int i=0; i < length; i++) {
						PartitionBucket bucket = partitionBuckets[i];
						columnFloatArray[i] = bucket.floatArrayIndex.get(columnName);
						nullStripArray[i] = bucket.nullStrips.get(columnName);
					}
					return new ColumnFilterFloatUniqueImpl(columnName, columnFloatArray, nullStripArray, resultBitset, bitsetCache);
				} else {
					MapWrapper[] invertedIndexMapper = new MapWrapper[length];
					for(int i=0; i < length; i++) {
						PartitionBucket bucket = partitionBuckets[i];
						invertedIndexMapper[i] = new MapWrapper(bucket.getInvertedIndex());
					}
					return new ColumnFilterFloatRepeatedImpl(columnName, invertedIndexMapper, dictionaryHolder.floatDictionary.get(columnName), resultBitset, bitsetCache);
				}
			}
		} finally {
			metrics.measureMethodCall(RoadRunnerUtils.format("searcher_" + columnName), (System.nanoTime()-start), true);
		}

	}

	@Override
	public ColumnFilterDouble whereDouble(String columnName) {

		if(StringUtils.isEmpty(columnName)) {
			metrics.increment("searcher_whereDouble_invalid_columnName", 1);
			logger.error("columnName is null / empty in whereDouble search: {}", columnName);
			throw new IllegalArgumentException("columnName is invalid");
		}

		long start = System.nanoTime();
		try {

			IndexableField fld = columns.get(columnName);
			int length = partitionBuckets.length;
			if(fld == null || ! useIndexFilter) {

				metrics.increment(RoadRunnerUtils.format("searcher_whereDouble_non_index_access_" + columnName), 1);
				RecordHolder[][] recordHolders = new RecordHolder[length][];
				String[] partitionBucketIds = new String[length];
				for(int i=0; i < length; i++) {
					PartitionBucket bucket = partitionBuckets[i];
					recordHolders[i] = bucket.getRecordHolders();
					partitionBucketIds[i] = bucket.id;
				}
				return new ColumnFilterDoubleNonIndexedImpl(partitionBucketIds, columnName, recordHolders, resultBitset, this.bitsetCache);

			} else {
				if (fld.isMostlyUnique()) {
					double[][] columnDoubleArray = new double[length][];
					BitSetExposed[] nullStripArray = new BitSetExposed[length];
					for(int i=0; i < length; i++) {
						PartitionBucket bucket = partitionBuckets[i];
						columnDoubleArray[i] = bucket.doubleArrayIndex.get(columnName);
						nullStripArray[i] = bucket.nullStrips.get(columnName);
					}
					return new ColumnFilterDoubleUniqueImpl(columnName, columnDoubleArray, nullStripArray, resultBitset, bitsetCache);
				} else {
					MapWrapper[] invertedIndexMapper = new MapWrapper[length];
					for(int i=0; i < length; i++) {
						PartitionBucket bucket = partitionBuckets[i];
						invertedIndexMapper[i] = new MapWrapper(bucket.getInvertedIndex());
					}
					return new ColumnFilterDoubleRepeatedImpl(columnName, invertedIndexMapper, dictionaryHolder.doubleDictionary.get(columnName), resultBitset, bitsetCache);
				}
			}
		} finally {
			metrics.measureMethodCall(RoadRunnerUtils.format("searcher_" + columnName), (System.nanoTime()-start), true);
		}
	}

	@Override
	public ColumnFilterLong whereLong(String columnName) {

		if(StringUtils.isEmpty(columnName)) {
			metrics.increment("searcher_whereLong_invalid_columnName", 1);
			logger.error("columnName is null / empty in whereLong search: {}", columnName);
			throw new IllegalArgumentException("columnName is invalid");
		}

		long start = System.nanoTime();
		try {

			IndexableField fld = columns.get(columnName);
			int length = partitionBuckets.length;
			if(fld == null || ! useIndexFilter) {

				metrics.increment(RoadRunnerUtils.format("searcher_whereLong_non_index_access_" + columnName), 1);
				RecordHolder[][] recordHolders = new RecordHolder[length][];
				String[] partitionBucketIds = new String[length];
				for(int i=0; i < length; i++) {
					PartitionBucket bucket = partitionBuckets[i];
					recordHolders[i] = bucket.getRecordHolders();
					partitionBucketIds[i] = bucket.id;
				}
				return new ColumnFilterLongNonIndexedImpl(partitionBucketIds, columnName, recordHolders, resultBitset, this.bitsetCache);

			} else {
				if (fld.isMostlyUnique()) {
					long[][] columnLongArray = new long[length][];
					BitSetExposed[] nullStripArray = new BitSetExposed[length];
					for(int i=0; i < length; i++) {
						PartitionBucket bucket = partitionBuckets[i];
						columnLongArray[i] = bucket.longArrayIndex.get(columnName);
						nullStripArray[i] = bucket.nullStrips.get(columnName);
					}
					return new ColumnFilterLongUniqueImpl(columnName, columnLongArray, nullStripArray, resultBitset, this.bitsetCache);
				} else {
					MapWrapper[] invertedIndexMapper = new MapWrapper[length];
					for(int i=0; i < length; i++) {
						PartitionBucket bucket = partitionBuckets[i];
						invertedIndexMapper[i] = new MapWrapper(bucket.getInvertedIndex());
					}
					return new ColumnFilterLongRepeatedImpl(columnName, invertedIndexMapper, dictionaryHolder.longDictionary.get(columnName), resultBitset, bitsetCache);
				}
			}
		} finally {
			metrics.measureMethodCall(RoadRunnerUtils.format("searcher_" + columnName), (System.nanoTime()-start), true);
		}
	}

	@Override
	public ColumnFilterString whereString(String columnName) {

		if(StringUtils.isEmpty(columnName)) {
			metrics.increment("searcher_whereString_invalid_columnName", 1);
			logger.error("columnName is null / empty in whereString search: {}", columnName);
			throw new IllegalArgumentException("columnName is invalid");
		}

		long start = System.nanoTime();
		try {
			IndexableField fld = columns.get(columnName);
			int length = partitionBuckets.length;
			if(fld == null || ! useIndexFilter) {

				metrics.increment(RoadRunnerUtils.format("searcher_whereString_non_index_access_" + columnName), 1);
				RecordHolder[][] recordHolders = new RecordHolder[length][];
				String[] partitionBucketIds = new String[length];
				for(int i=0; i < length; i++) {
					PartitionBucket bucket = partitionBuckets[i];
					recordHolders[i] = bucket.getRecordHolders();
					partitionBucketIds[i] = bucket.id;
				}
				return new ColumnFilterStringNonIndexedImpl(partitionBucketIds, columnName, recordHolders, resultBitset, this.bitsetCache);

			} else {
				if (fld.isMostlyUnique()) {
					String[][] columnStringArray = new String[length][];
					BitSetExposed[] nullStripArray = new BitSetExposed[length];
					for(int i=0; i < length; i++) {
						PartitionBucket bucket = partitionBuckets[i];
						columnStringArray[i] = bucket.StringArrayIndex.get(columnName);
						nullStripArray[i] = bucket.nullStrips.get(columnName);
					}
					return new ColumnFilterStringUniqueImpl(columnName, columnStringArray, nullStripArray, resultBitset, this.bitsetCache);

				} else {
					MapWrapper[] invertedIndexMapper = new MapWrapper[length];
					for(int i=0; i < length; i++) {
						PartitionBucket bucket = partitionBuckets[i];
						invertedIndexMapper[i] = new MapWrapper(bucket.getInvertedIndex());
					}
					return new ColumnFilterStringRepeatedImpl(columnName, invertedIndexMapper, dictionaryHolder.stringDictionary.get(columnName),
							resultBitset, bitsetCache);
				}
			}
		} finally {
			metrics.measureMethodCall(RoadRunnerUtils.format("searcher_" + columnName), (System.nanoTime()-start), true);
		}
	}

	/* (non-Javadoc)
	 * @see com.ola.storage.api.GeoSpatialSearcher#records(com.ola.storage.api.RecordIndexes)
	 */
	@Override
	public final Records records(final RecordIndexes indexes) {
		BitSetExposed[] intermediateBitSet = indexes.getIds();
		int expectedRecordsSize = 0;
		for ( BitSetExposed bits : intermediateBitSet) {
			expectedRecordsSize += bits.cardinality();
		}
		Records records = new RecordsImpl(expectedRecordsSize+1);
		this.forEach(indexes, new Consumer<Record>() {
			@Override
			public final void accept(final Record record) {
				records.add(record);
			}
		});

		return records;
	}

	@Override
	public void forEach(RecordIndexes indexes, Consumer<Record> action) {
		BitSetExposed[] intermediateBitSet = indexes.getIds();
		int size = intermediateBitSet.length;
		Set<String> recordIdsCollected = new HashSet<>();
		for(int i = 0; i < size; i++) {

			PartitionBucket bucket = partitionBuckets[i];
			BitSetExposed bs = intermediateBitSet[i];

			RecordHolder[] recordHolders = bucket.getRecordHolders();
			int recordsMaxCapacity = recordHolders.length;
			long[] words = bs.toLongArrayExposed();
			int wordsT = words.length;
			String bucketId = bucket.id;

			int recordsT = bs.cardinality();
			int recordI = 0;

			int maxWordIndex = (recordsMaxCapacity / 64) + 1;
			for(int wordIndex = 0; wordIndex < maxWordIndex; wordIndex++) {
				if (wordIndex >= wordsT) break;
				long word = words[wordIndex];
				int bitStartIndex = wordIndex * 64;
				int bitEndIndex =  bitStartIndex + 64;
				if(bitEndIndex > recordsMaxCapacity) bitEndIndex = recordsMaxCapacity;

				recordI = onRecord(action, recordIdsCollected, recordHolders, bucketId, recordsT, recordI, word, bitStartIndex, bitEndIndex);
				if ( recordI >= recordsT) break;
			}
		}
	}

	private final int onRecord(final Consumer<Record> action, Set<String> recordIdsCollected, final RecordHolder[] recordHolders, final String bucketId, final int recordsT,
			int recordI, final long word, final int bitStartIndex, final int bitEndIndex) {

		for (int bitIndex = bitStartIndex; bitIndex < bitEndIndex; bitIndex++) {
			boolean bitVal = ((word & (1L << bitIndex)) != 0);
			if (bitVal) {
				Record record = recordHolders[bitIndex].get(bucketId, bitIndex);
				if(record != null) {
					String id = record.getId();
					if(! recordIdsCollected.contains(id)) {
						action.accept(record);
						recordIdsCollected.add(id);
					}
				}
				recordI++;
			} 
			if ( recordI >= recordsT) break;
		}
		return recordI;
	}

	@Override
	public Stream<Record> stream(RecordIndexes indexes) {
		long start = System.nanoTime();
		Stream.Builder<Record> records = Stream.builder();
		boolean success = false;
		try {
			this.forEach(indexes, new Consumer<Record>() {
				@Override
				public final void accept(final Record record) {
					records.add(record);
				}
			});
			Stream<Record> recordStream = records.build();
			success = true;
			return recordStream;
		}
		finally {
			metrics.measureMethodCall("searcher_stream", (System.nanoTime()-start), success);
		}
	}

	@Override
	public final <T extends Object> Map<T, List<Record>> stream(RecordIndexes indexes, List<Predicate<Record>> predicates, 
			String groupingBy, Map<T, List<Record>> groupedRecords) throws IllegalArgumentException {

		Stream<Record> recordStream = stream(indexes);

		long start = System.nanoTime();
		if(predicates != null) {
			boolean success = false;
			try {
				for(Predicate<Record> predicate : predicates) {
					recordStream = recordStream.filter(predicate);
				}
				success = true;
			} finally {
				metrics.measureMethodCall("searcher_business_filter" , (System.nanoTime() -start), success);
			}
		}

		if ( StringUtils.isEmpty(groupingBy)) {
			throw new IllegalArgumentException("groupingBy null not allowed.");
		}

		start = System.nanoTime();
		boolean success = false;
		try {
			recordStream.forEach(record -> {
				Object obj = record.getTokenizedField(groupingBy);
				if (obj != null) {
					if (obj instanceof List) {
						List<T> valueList = (List<T>) obj;
						for (T value : valueList) {
							addToGroupedRecords(groupedRecords, record, value);
						}
					} else {
						addToGroupedRecords(groupedRecords, record, (T) obj);
					}
				}
			});
			success = true;
		} finally {
			metrics.measureMethodCall("searcher_group" , (System.nanoTime()-start), success);
		}

		return groupedRecords;
	}

	public Stream<Record> stream(RecordIndexes indexes, List<Predicate<Record>> predicates, String sortingBy, int limit) {

		Stream<Record> recordStream = stream(indexes);

		boolean success = false;
		long start = System.nanoTime();
		if(predicates != null) {
			try {
				for(Predicate<Record> predicate : predicates) {
					recordStream = recordStream.filter(predicate);
				}
				success = true;
			} finally {
				metrics.measureMethodCall("searcher_business_filter" , (System.nanoTime() -start), success);
			}
		}

		success = false;
		start = System.nanoTime();
		try {

			if (StringUtils.isNotEmpty(sortingBy)) {
				recordStream = recordStream.sorted(Record.buildComparator(sortingBy));
			}

			boolean shouldLimit = !(limit == 0 || limit == -1 || Integer.MAX_VALUE == limit);
			if (shouldLimit) {
				recordStream = recordStream.limit(limit);
			}

			success = true;
			return recordStream;

		} finally {
			metrics.measureMethodCall("searcher_sortlimit" , (System.nanoTime()-start), success);
		}
	}

	@Override
	public Stream<GeoSpatialRecord> streamGeoSpatial(RecordIndexes indexes) {

		long start = System.nanoTime();
		recordCount.clear();
		try {

			BitSetExposed[] intermediateBitSet = indexes.getIds();
			int expectedRecordsSize = 0;
			for ( BitSetExposed bits : intermediateBitSet) {
				expectedRecordsSize += bits.cardinality();
			}

			final double pickupLat = this.latitude;
			final double pickupLon = this.longitude;
			final double pickupLatCosine = Haversine.cosapprox(pickupLat * Haversine.PIBY180);
			List<GeoSpatialRecord> records = new ArrayList<>(expectedRecordsSize+1);
			final int radius = (pickupLat == 0.0 && pickupLon == 0.0 && this.radiusInMeters == 0.0 ) ? -1 : this.radiusInMeters;
			this.forEach(indexes, new Consumer<Record>() {
				@Override
				public final void accept(final Record record) {
					GeoSpatialRecord gRecord = geospatialRecordCache.take();
					gRecord.set(record, pickupLat, pickupLon, pickupLatCosine);
					if ( radius == -1 ) {
						records.add(gRecord);
						recordCount.includedRecords++;
					} else {
						double distance = gRecord.distanceInMetersCache();
						if (distance <= radius) {
							records.add(gRecord);
							recordCount.includedRecords++;
						} else {
							recordCount.excludedRecords++;
						}
					}
				}
			});
			return records.stream();

		} finally {
			metrics.measureMethodCall("searcher_streamGeoSpatial_IncludedRecords", recordCount.includedRecords, true);
			metrics.measureMethodCall("searcher_streamGeoSpatial_ExcludedRecords", recordCount.excludedRecords, true);
			metrics.measureMethodCall("searcher_streamGeoSpatial", (System.nanoTime()-start), true);
		}

	}

	protected GeoSpacialSearcherImpl clear() {
		bitsetCache.reclaim();
		geospatialRecordCache.reclaim();
		return this;
	}

	@Override
	public final <T extends Object> Map<T, List<GeoSpatialRecord>> streamGeoSpatial(RecordIndexes indexes,
			List<Predicate<GeoSpatialRecord>> predicates,
			String groupingBy, Map<T, List<GeoSpatialRecord>> groupedRecords) throws IllegalArgumentException {

		Stream<GeoSpatialRecord> recordStream = streamGeoSpatial(indexes);

		long start = System.nanoTime();
		if(predicates != null) {
			boolean success = false;
			try {
				Stream<GeoSpatialRecord> recordStreamFiltered = null;
				for(Predicate<GeoSpatialRecord> predicate : predicates) {
					recordStreamFiltered = recordStream.filter(predicate);
					recordStream = recordStreamFiltered;
				}
				success = true;
			} finally {
				metrics.measureMethodCall("searcher_business_filter" , (System.nanoTime() -start), success);
			}
		}

		if ( StringUtils.isEmpty(groupingBy)) {
			throw new IllegalArgumentException("groupingBy null not allowed. Use streamGeoSpatial(RecordIndexes indexes, String sortingBy, int limit) ");
		}

		start = System.nanoTime();
		boolean success = false;
		try {
			recordStream.forEach(geoSpatialRecord -> {
				Object obj = geoSpatialRecord.record.getTokenizedField(groupingBy);
				if (obj != null) {
					if (obj instanceof List) {
						List<T> valueList = (List<T>) obj;
						for (T value : valueList) {
							addToGroupedGeoSpatialRecords(groupedRecords, geoSpatialRecord, value);
						}
					} else {
						addToGroupedGeoSpatialRecords(groupedRecords, geoSpatialRecord, (T) obj);
					}
				}
			});
			success = true;
		} finally {
			metrics.measureMethodCall("searcher_group" , (System.nanoTime()-start), success);
		}

		return groupedRecords;
	}

	@Override
	public final <T extends Object> Map<T, List<GeoSpatialRecord>> streamGeoSpatial(RecordIndexes indexes, 
			List<Predicate<GeoSpatialRecord>> predicates, 
			String groupingBy, String sortingBy, int limit, 
			Map<T, List<GeoSpatialRecord>> groupedRecords) throws IllegalArgumentException {

		groupedRecords = streamGeoSpatial(indexes, predicates, groupingBy, groupedRecords);

		long start;
		boolean success;

		boolean shouldLimit = false;
		if(limit > 0) {
			for( List<GeoSpatialRecord> records : groupedRecords.values()) {
				if ( null == records) continue;
				if ( records.size() > limit) {
					shouldLimit = true;
					break;
				}
			}
		}

		if(StringUtils.isNotEmpty(sortingBy)) {
			start = System.nanoTime();
			success = false;
			try {

				for (Map.Entry<T, List<GeoSpatialRecord>> entry : groupedRecords.entrySet() ) {
					List<GeoSpatialRecord> records = entry.getValue();
					if(sortingBy.equalsIgnoreCase(GeoSpatialRecord.DISTANCE)) {
						if(shouldLimit) { 
							entry.setValue(records.stream().sorted().limit(limit).collect(Collectors.toList()));
						} else {
							entry.setValue(records.stream().sorted().collect(Collectors.toList()));
						}
					} else {
						if(shouldLimit) { 
							try {
								entry.setValue(records.stream().sorted(GeoSpatialRecord.buildComparator(sortingBy)).limit(limit).collect(Collectors.toList()));

							} catch(Exception e) {
								metrics.increment("searcher_group_sorting_failed", 1);
								logger.error("Error in sorting records : {}, Error : {}", records, e.getMessage(), e);
								entry.setValue(records.stream().limit(limit).collect(Collectors.toList()));
							}
						} else {
							try {
								entry.setValue(records.stream().sorted(GeoSpatialRecord.buildComparator(sortingBy)).collect(Collectors.toList()));

							} catch(Exception e) {
								metrics.increment("searcher_group_sorting_failed", 1);
								logger.error("Error in sorting records : {}, Error : {}", records, e.getMessage(), e);
								entry.setValue(records.stream().collect(Collectors.toList()));
							}
						}
					}
				}

				success = true;

			} finally {
				metrics.measureMethodCall("searcher_groupsorting" , (System.nanoTime()-start), success);
			}
		} else if ( shouldLimit ) {
			success = false;
			start = System.nanoTime();
			try {

				for (Map.Entry<T, List<GeoSpatialRecord>> entry : groupedRecords.entrySet() ) {
					List<GeoSpatialRecord> records = entry.getValue();
					if (records.size() > limit) {
						entry.setValue(records.subList(0, limit));
					}
				}
				success = true;

			} finally {
				metrics.measureMethodCall("searcher_grouplimit", (System.nanoTime()-start), success);
			}
		}

		return groupedRecords;
	}

	public Stream<GeoSpatialRecord> streamGeoSpatial(RecordIndexes indexes, List<Predicate<GeoSpatialRecord>> predicates, 
			String sortingBy, int limit) {

		Stream<GeoSpatialRecord> recordStream = streamGeoSpatial(indexes);

		boolean success = false;
		long start = System.nanoTime();
		if(predicates != null) {
			try {
				for(Predicate<GeoSpatialRecord> predicate : predicates) {
					Stream<GeoSpatialRecord> recordStreamFiltered = recordStream.filter(predicate);
					recordStream = recordStreamFiltered;
				}
				success = true;
			} finally {
				metrics.measureMethodCall("searcher_business_filter" , (System.nanoTime() -start), success);
			}
		}

		success = false;
		start = System.nanoTime();
		try {

			if (StringUtils.isNotEmpty(sortingBy)) {
				if (sortingBy.equalsIgnoreCase(GeoSpatialRecord.DISTANCE)) {
					Stream<GeoSpatialRecord> recordStreamSorted = recordStream.sorted();
					recordStream = recordStreamSorted;
				} else {
					Stream<GeoSpatialRecord> recordStreamSorted = recordStream.sorted(GeoSpatialRecord.buildComparator(sortingBy));
					recordStream = recordStreamSorted;
				}
			}

			boolean shouldLimit = !(limit == 0 || limit == -1 || Integer.MAX_VALUE == limit);
			if (shouldLimit) {
				Stream<GeoSpatialRecord> recordStreamLimited = recordStream.limit(limit);
				recordStream = recordStreamLimited;
			}

			success = true;
			return recordStream;

		} finally {
			metrics.measureMethodCall("searcher_sortlimit" , (System.nanoTime()-start), success);
		}
	}


	private <T> void addToGroupedGeoSpatialRecords(Map<T, List<GeoSpatialRecord>> groupedRecords, GeoSpatialRecord geoSpatialRecord, T obj) {
		List<GeoSpatialRecord> recordList = groupedRecords.get(obj);
		if(recordList == null) {
			recordList = new ArrayList<>();
			groupedRecords.put(obj, recordList);
		}
		recordList.add(geoSpatialRecord);
	}

	private <T> void addToGroupedRecords(Map<T, List<Record>> groupedRecords, Record geoSpatialRecord, T obj) {
		List<Record> recordList = groupedRecords.get(obj);
		if(recordList == null) {
			recordList = new ArrayList<>();
			groupedRecords.put(obj, recordList);
		}
		recordList.add(geoSpatialRecord);
	}

}
