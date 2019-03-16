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

import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import com.newrelic.api.agent.Trace;
import com.olacabs.BitSetExposed;
import com.olacabs.roadrunner.api.IndexableField;
import com.olacabs.roadrunner.api.Record;
import com.olacabs.roadrunner.monitor.RoadRunnerMetricFactory;
import com.olacabs.roadrunner.utils.Logger;
import com.olacabs.roadrunner.utils.LoggerFactory;

public class PartitionBucket extends PartitionAbstract {


	public static long COOL_OF_PERIOD = 5000;
	public static int INITIAL_CAPACITY = 4;
	public static int SUBSEQUENT_INCREMENT = 128; //increment_state=2

	public final static String SEPARATOR = "_";
	public final static int NOT_INITIALIZED = -2;
	public final static int NOT_AVAILABLE = -1;

	private static RoadRunnerMetricFactory metrics = RoadRunnerMetricFactory.getInstance();
	private static final int CLEANUP_WAIT_TIMES = 2;
	private static Logger logger = LoggerFactory.getLogger(PartitionBucket.class.getName());

	String id;
	int cleanup = 0;
	int lastForceCleanupBefore = 0;

	private Collection<IndexableField<?>> indexableFields;
	private Map<String, IndexableField<?>> columns;

	private RecordHolder[] recordHolders;
	private Map<String, BitSetExposed> invertedIndex;
	private BitSetExposed availableBits;
	
	private long[] updatedTimestamp;
	
	private long lastTouchedTime;

	public PartitionBucket(Map<String, IndexableField<?>> columns, String id) {
		super();
		this.columns = columns;
		this.indexableFields = columns.values();
		this.id = id;
		
		super.capacity = INITIAL_CAPACITY;
		recordHolders = new RecordHolder[INITIAL_CAPACITY];
		updatedTimestamp = new long[INITIAL_CAPACITY];
		this.availableBits = new BitSetExposed();
		
		this.invertedIndex = new HashMap<String, BitSetExposed>();   //fieldname_fieldval and values
		
		longArrayIndex = new HashMap<String, long[]>();  //fieldname and values
		intArrayIndex = new HashMap<String, int[]>();     //fieldname and values
		shortArrayIndex = new HashMap<String, short[]>();  //fieldname and values
		floatArrayIndex = new HashMap<String, float[]>();   //fieldname and values
		doubleArrayIndex = new HashMap<String, double[]>();   //fieldname and values
		StringArrayIndex = new HashMap<String, String[]>();   //fieldname and values
		
		nullStrips = new HashMap<String, BitSetExposed>();
		initializeNullStrip();
		this.lastTouchedTime = System.currentTimeMillis();
	}
	
	public PartitionBucket(String id, RecordHolder[] recordHolders) {
		super();
		this.id = id;
		int size = recordHolders.length;
		super.capacity = size;
		this.recordHolders = recordHolders;
		updatedTimestamp = new long[size];
		this.availableBits = new BitSetExposed(size);
		
		long currentTime = System.currentTimeMillis();
		for(int i = 0; i < size; i++) {
			updatedTimestamp[i] = currentTime;
			availableBits.set(i);
		}
		this.lastTouchedTime = currentTime;
	}

	
	
	private void initializeNullStrip() {
		for(IndexableField<?> field : indexableFields) {
			if(field.isMostlyUnique()) {
				nullStrips.put(field.getName(), new BitSetExposed());
			}
		}
	}
	
	public long getLastTouchedTime() {
		return this.lastTouchedTime;
	}
	
	public long getUpdatedTimestamp(int pos) {
		return this.updatedTimestamp[pos];
	}
	
	public final void onReceive(final PartitionBucketMessage s2msg) throws Exception {
		
		metrics.increment("s2received", 1);
		long start = System.nanoTime();
		try {
			switch (s2msg.type) {

			case INSERTCOMMIT:

				/**
				 * An device requested for insert @ cell1 and immediately insert at cell2.
				 * cell1 has long queue resulted in delayed processing and cell2 has short queue on early processing.
				 * In this case, the cellId will not be same while inserting. This case, do double check at build and end of processing.
				 * If anything gets processed it will be jombie which will be taken care during the checksum processing.
				 */
				this.lastTouchedTime = System.currentTimeMillis();
				int pos = this.insert(s2msg.holder, s2msg.receivedRecord, s2msg.flattenReceivedRecord, false);
				this.insertCommit(pos);
				metrics.measureMethodCall("s2insertCommit", (System.nanoTime() - start), true);

				if ( ! this.id.equals(s2msg.holder.getNewRecord().getPartitionValue())) {
					metrics.increment("s2insertuseless_atend", 1);
					this.softDelete(pos);
				}

				break;

			case TOUCH:
				this.lastTouchedTime = System.currentTimeMillis();
				RecordImplVersion record = s2msg.existingRecord;
				if ( ! this.id.equals(record.getPartitionValue())) {
					metrics.increment("s2mismatchtouch", 1);
					break;
				}
				this.touch(record.getPOS(), s2msg.flattenExistingRecord, s2msg.flattenReceivedRecord);
				metrics.measureMethodCall("s2touch", (System.nanoTime()-start), true);
				break;

			case INSERTDELETE:

				/**
				 * An device requested for insert @ cell1 and immediately moved to cell2 resulting a insertdelete in cell1
				 * cell1 has long queue resulted in delayed processing and cell2 has short queue on early processing.
				 * In this case, the cellId will not be same while inserting. This case, do double check at build and end of processing.
				 * If anything gets processed it will be jombie which will be taken care during the checksum processing.
				 */
				this.lastTouchedTime = System.currentTimeMillis();
				int newpos = this.insert(s2msg.holder, s2msg.receivedRecord, s2msg.flattenReceivedRecord, true);
				this.insertCommit(newpos);

				softDelete(s2msg.existingRecord.getPOS());
				metrics.measureMethodCall("s2insertDelete", (System.nanoTime()-start), true);

				if ( ! this.id.equals(s2msg.holder.getNewRecord().getPartitionValue())) {
					metrics.increment("s2insertDeleteuseless_atend", 1);
					this.softDelete(newpos);
				}

				break;

			case SOFTDELETE:
				RecordImplVersion existingRecord = s2msg.existingRecord;
				
				if ( existingRecord != null && existingRecord.getPOS() >= 0) {
					int posToDelete = existingRecord.getPOS();
					RecordHolder recordHolder = this.recordHolders[posToDelete];
					
					if(recordHolder != null && recordHolder.getId().equals(existingRecord.getId())) {
						this.softDelete(posToDelete);
						metrics.measureMethodCall("s2softDelete", (System.nanoTime()-start), true);
					}
				} else {
					metrics.measureMethodCall("s2softDelete", (System.nanoTime()-start), false);
				}
				break;

			case CLEANUP:
				int collected = this.cleanup();
				metrics.measureSummary("s2CleanupAfterCleanupInterval", collected);
				metrics.measureMethodCall("s2cleanup", (System.nanoTime()-start), true);
				break;

			case CHECKSUM:
				boolean success = this.checksum();
				metrics.measureMethodCall("s2checksum", (System.nanoTime()-start), success);
				break;

			default:
				metrics.increment("s2unknown", 1);
				throw new Exception("Unknown message type: " + s2msg.type.name());
			}
		} catch(Exception e) {
			logger.error("Exception in processing PartitionBucketMessage onReceive : {}. Error : {}", s2msg, e.getMessage(), e);
			throw e;
		}
	}

	private int insert(RecordHolder recordHolder,
					   RecordImplVersion receivedRecord, RecordImplVersion receivedRecordFlattened, boolean trace) {

		int insertPos = NOT_AVAILABLE;
		long startNano = -1;

		try {
			if ( trace ) startNano = System.nanoTime();
			insertPos = findInsertPos(trace);
			if ( trace ) {
				long endNano = System.nanoTime();
				metrics.measureMethodCall("s2insertFindpos", (endNano-startNano), true);
				startNano = endNano;
			}

			/**
			 * While we are changing the position, the hashmap may undergo read operation to flatten
			 * existing records for a merge metadata change. Map-Read in caller thread and map-write in
			 * this thread will cause concurrency exception. So all updates here should not touch the record-map.
			 */
			receivedRecord.setPos(insertPos);
			receivedRecordFlattened.setPos(insertPos);

			recordHolders[insertPos] = recordHolder;
			updatedTimestamp[insertPos] = System.currentTimeMillis();
			buildInvertedIndex(receivedRecordFlattened);

			if ( trace ) {
				long endNano = System.nanoTime();
				metrics.measureMethodCall("s2insertInvertedIndex", (endNano-startNano), true);
				startNano = endNano;
			}

			buildArrayIndex(receivedRecordFlattened);

			if ( trace ) {
				long endNano = System.nanoTime();
				metrics.measureMethodCall("s2insertArrayIndex", (endNano-startNano), true);
			}

			receivedRecordFlattened.clear();
			return insertPos;

		} catch (ArrayIndexOutOfBoundsException ex) {
			metrics.increment("s2insertArrayIndexOutOfBoundsException", 1);
			logger.error("Error while insert {} \n", receivedRecordFlattened, ex);
			throw new ArrayIndexOutOfBoundsException(
					", insertpos:" + insertPos + ", recordHolders=" + recordHolders.length +
					", updatedTimestamp:" + updatedTimestamp.length);
		}
	}

	private int findInsertPos(boolean trace) {
		int insertPos;
		insertPos = NOT_AVAILABLE;
		lastForceCleanupBefore++;
		int recordsMaxCapacity = recordHolders.length;

		//Check if any empty record.
		for (int pos = 0; pos < recordsMaxCapacity; pos++) {

			if (recordHolders[pos] == null || recordHolders[pos].isEmpty()) {
				insertPos = pos;
				break;
			}
		}
/**	
		//Check if this this is a soft instrumentDelete position.
		if (insertPos == -1) {
            long currentTime = System.currentTimeMillis();
            for (int pos = availableBits.nextClearBit(0); pos >= 0; pos = availableBits.nextClearBit(pos + 1)) {
                if (pos >= length) break;
                if ((currentTime - updatedTimestamp[pos]) > COOL_OF_PERIOD) {
                    delete(pos);
                    insertPos = pos;
                    break;
                }
            }

        }
 */

		if (insertPos == NOT_AVAILABLE) {
			if ( lastForceCleanupBefore > INITIAL_CAPACITY) {
				int collected = this.cleanup();
				metrics.measureSummary("s2CleanupFindInsertPos", collected);
				lastForceCleanupBefore = 0;
			}
			
			if(insertPos == NOT_AVAILABLE) {
				long currentTime = System.currentTimeMillis();
				long[] words = availableBits.toLongArrayExposed();
				int wordsT = words.length;
				int recordsT = availableBits.cardinality(); //total setbits
				int recordI = 0;
				
				int maxWordIndex = (recordsMaxCapacity / 64) + 1;
				for(int wordIndex = 0; wordIndex < maxWordIndex; wordIndex++) {
					if (wordIndex >= wordsT) break;
					long word = words[wordIndex];
					int bitStartIndex = wordIndex * 64;
					int bitEndIndex =  bitStartIndex + 64;
					if(bitEndIndex > recordsMaxCapacity) bitEndIndex = recordsMaxCapacity;
					
					for (int bitIndex = bitStartIndex; bitIndex < bitEndIndex; bitIndex++) {
						boolean bitVal = ((word & (1L << bitIndex)) != 0);
						if (bitVal) {
							recordI++; //Bit is set
						} else {
							if ((currentTime - updatedTimestamp[bitIndex]) > COOL_OF_PERIOD) {
								delete(bitIndex);
								insertPos = bitIndex;
								break;
							}
						}
						if (recordI >= recordsT) break;
					}
					if (recordI >= recordsT) break;
				}
			}
		}

		if (insertPos == NOT_AVAILABLE) {
            insertPos = expand(trace);
        }
		return insertPos;
	}

	private int expand(boolean trace) {

		long start = System.nanoTime();

		int currentCapacity = recordHolders.length;
		int newCapacity = currentCapacity + SUBSEQUENT_INCREMENT;

		//instrument();

		expandMaps(currentCapacity, newCapacity);

//		Expanding updatedTimestamp array
		
		long[] newUpdatedTimestamp = new long[newCapacity];
		System.arraycopy(updatedTimestamp, 0, newUpdatedTimestamp, 0, currentCapacity);
		Arrays.fill(newUpdatedTimestamp, currentCapacity, newCapacity, -1);
		updatedTimestamp = newUpdatedTimestamp;

		//		Expanding record holder
		RecordHolder[] newHolder = new RecordHolder[newCapacity];
		System.arraycopy(recordHolders, 0, newHolder, 0, currentCapacity);
		recordHolders = newHolder;

		metrics.measureMethodCall("s2expand", (System.nanoTime()-start), true);
		if ( trace ) metrics.measureMethodCall("s2TraceExpand", (System.nanoTime()-start), true);

		return currentCapacity;
	}


	private void buildArrayIndex(RecordImplVersion recordImplVersion) {
		for(IndexableField<?> indexableField: indexableFields ) {
			if(indexableField.isMostlyUnique()) {
				updateArrayIndex(recordImplVersion, indexableField, recordHolders.length);
			}
		}
	}

	private void buildInvertedIndex(RecordImplVersion recordImplVersion) {
		for(IndexableField<?> indexableField: indexableFields ) {
			if(!indexableField.isMostlyUnique()) {
				int pos = recordImplVersion.getPOS();
				updateInvertedIndex(pos, indexableField, recordImplVersion, false);
			}
		}
	}

	private void insertCommit(int pos) {
		this.availableBits.set(pos);
	}

	private void touch(int pos, RecordImplVersion existingFlattenedRecord, RecordImplVersion receivedFlattenedRecord) {
		for(String key : receivedFlattenedRecord.keySet()) {
			
			IndexableField<?> indexableField = columns.get(key);
			if (null == indexableField) continue;
			
			if(indexableField.isThreadSafe()) {
				if(indexableField.isMostlyUnique()) {
					try {
						touchArrayIndex(receivedFlattenedRecord, pos, indexableField);
					} catch (ArrayIndexOutOfBoundsException ex) {
						metrics.increment("s2touchArrayIndexOutOfBoundsException", 1);
						logger.error("ArrayIndexOutOfBoundException during touch : {}", ex.getMessage(), ex);
						throw ex;
					}

				} else {
					updateInvertedIndex(pos, indexableField, existingFlattenedRecord, true);
					updateInvertedIndex(pos, indexableField, receivedFlattenedRecord, false);
				}
			}
		}
		receivedFlattenedRecord.clear();
		existingFlattenedRecord.clear();

		updatedTimestamp[pos] = System.currentTimeMillis();
	}
	
	private void addToInvertedIndex(String key, BitSetExposed value) {
		Map<String, BitSetExposed> map = new HashMap<>(this.invertedIndex);
		map.put(key, value);
		this.invertedIndex = map;
	}
	
	private void updateInvertedIndex(int pos, IndexableField<?> indexableField, RecordImplVersion record, boolean forceClear) {
		String fieldName = indexableField.getName();
		switch (indexableField.getDataType()) {
		
		case ARRSHORT:
			{
				short[] shortArray = record.getShortArrayField(fieldName);
				if(shortArray != null) {
					for(short value : shortArray) {
						updateInvertedIndexPos(fieldName, value, pos, forceClear);
					}
				} else {
					updateInvertedIndexPos(fieldName, shortArray, pos, forceClear);
				}
			}
			break;
			
		case ARRINT:
			{
				int[] intArray = record.getIntegerArrayField(fieldName);
				if(intArray != null) {
					for(int value : intArray) {
						updateInvertedIndexPos(fieldName, value, pos, forceClear);
					}
				} else {
					updateInvertedIndexPos(fieldName, intArray, pos, forceClear);
				}
			}
			break;
		
		case ARRFLOAT:
			{
				float[] floatArray = record.getFloatArrayField(fieldName);
				if(floatArray != null) {
					for(float value : floatArray) {
						updateInvertedIndexPos(fieldName, value, pos, forceClear);
					}
				} else {
					updateInvertedIndexPos(fieldName, floatArray, pos, forceClear);
				}
			}
			break;
		
		case ARRLONG:
			{
				long[] longArray = record.getLongArrayField(fieldName);
				if(longArray != null) {
					for(long value : longArray) {
						updateInvertedIndexPos(fieldName, value, pos, forceClear);
					}
				} else {
					updateInvertedIndexPos(fieldName, longArray, pos, forceClear);
				}
			}
			break;
		
		case ARRDOUBLE:
			{
				double[] doubleArray = record.getDoubleArrayField(fieldName);
				if(doubleArray != null) {
					for(double value : doubleArray) {
						updateInvertedIndexPos(fieldName, value, pos, forceClear);
					}
				} else {
					updateInvertedIndexPos(fieldName, doubleArray, pos, forceClear);
				}
			}
			break;
		
		case ARRSTRING:
			{
				String[] stringArray = record.getStringArrayField(fieldName);
				if(stringArray != null) {
					for(String value : stringArray) {
						updateInvertedIndexPos(fieldName, value, pos, forceClear);
					}
				} else {
					updateInvertedIndexPos(fieldName, stringArray, pos, forceClear);
				}
			}
			break;
			
		default:
			Object value = record.getField(fieldName);
			updateInvertedIndexPos(fieldName, value, pos, forceClear);
			break;
		}
	}

	private void updateInvertedIndexPos(String fieldName, Object value, int pos, boolean forceClear) {
		String key = fieldName + SEPARATOR + String.valueOf(value);
		if(invertedIndex.get(key) == null) {
			addToInvertedIndex(key, new BitSetExposed(capacity));
		}
		
		if(forceClear) {
			invertedIndex.get(key).clear(pos);
		} else {
			invertedIndex.get(key).set(pos);
		}
	}

	private void softDelete(int pos) {
		this.availableBits.clear(pos);
	}
	
	@Trace(dispatcher = true)
	private void delete(int pos) {
		recordHolders[pos] = RecordHolder.EMPTY;
		
		for(Entry<String, BitSetExposed> entry: invertedIndex.entrySet()) {
			entry.getValue().clear(pos);
		}
		
		deleteArrayIndex(pos);
		
		updatedTimestamp[pos] = -1;
	}

//	TODO : build index for existing record in cell
	public void addIndexableField(IndexableField<?> field) {
	}

	public void healthCheck() {
		//Check if this this is a soft instrumentDelete position.
		long currentTime = System.currentTimeMillis();
		for(int pos = availableBits.nextClearBit(0); pos >= 0; pos = availableBits.nextClearBit(pos + 1)) {
			if ( availableBits.length() == pos) break;
			if( (currentTime - updatedTimestamp[pos]) > COOL_OF_PERIOD ) {
				delete(pos);
			}
		}
	}

	private int cleanup() {
		int collected = 0;
		long currentTime = System.currentTimeMillis();
		int total = ( recordHolders.length > updatedTimestamp.length ) ? updatedTimestamp.length: recordHolders.length;

		long coolOffPeriodTimes = CLEANUP_WAIT_TIMES * COOL_OF_PERIOD;
			
		for(int pos=0; pos<total; pos++) {
			if ( availableBits.get(pos)) continue;
			if ( null == recordHolders[pos] ) continue;
			if ( recordHolders[pos].isEmpty() ) continue;

			if( (currentTime - updatedTimestamp[pos]) > coolOffPeriodTimes ) {
				delete(pos);
				collected++;
			}
		}
		
		this.cleanup = collected;
		return collected;
	}

	private boolean checksum() {
		boolean success = false;
		try {
			int length = recordHolders.length;
			long currentTime = System.currentTimeMillis();
			long coolOffPeriodTimes = CLEANUP_WAIT_TIMES * COOL_OF_PERIOD;
			int collected = 0;

			for(int pos = 0; pos < length; pos++) {
				if ( (currentTime - updatedTimestamp[pos]) < coolOffPeriodTimes  ) continue;
				RecordHolder recordHolder = recordHolders[pos];
				if ( null == recordHolder ) continue;
				if ( recordHolder.isEmpty() ) continue;

				Record newRecord = recordHolder.getNewRecord();
				if (null == newRecord) continue;

				if ( ! availableBits.get(pos)) continue;
				if( this.id.equals( newRecord.getPartitionValue() ) ) continue;

				collected++;  //Collect the zombies

			}
			if(collected != 0) {
				metrics.measureSummary("s2ChecksumCollected", collected);
			} 
			success = true;
		
		} catch(Exception e) {
			logger.error("Error in checksum : {}", e.getMessage(), e);
		}
		return success;
	}

	public void instrument(final Map<String, Long> metrics, final String tagName) {

		int currentCapacity = recordHolders.length;
		Set<String> idsUnique = new HashSet<String>();
		int latest = 0;
		int expired = 0;
		int empty = 0;
		int celljump = 0;

		int celljumpOnce = 0;
		int celljumpTwice = 0;

		int celljumplatest = 0;
		int celljumpexpired = 0;

		int position = -1;
		for ( RecordHolder rh : recordHolders) {
			position++;
			if ( null == rh) continue;

			if (null == rh.getNewRecord()) {
				empty++;
				continue;
			}
			idsUnique.add(rh.getNewRecord().getId());
			if (this.id.equals(rh.getNewRecord().getPartitionValue())) {
				int pos = rh.getNewRecord().getPOS();
				if ( pos >= 0 ) {
					if (updatedTimestamp[pos] - System.currentTimeMillis() < 5000) latest++;
					else expired++;
				}
			} else {
				celljump++;
				if ( rh.getOldRecord().getPartitionValue().equals(this.id)) {
					int pos = rh.getOldRecord().getPOS();
					if ( pos >= 0 ) {
						if (updatedTimestamp[pos] - System.currentTimeMillis() < 5000) celljumplatest++;
						else celljumpexpired++;
					}
				} else {
					this.availableBits.get(position);
					if (updatedTimestamp[position] - System.currentTimeMillis() < 5000) celljumplatest++;

					if ( rh.getOldRecord().getPartitionValue().equals(rh.getNewRecord().getPartitionValue()))   celljumpOnce++;
					else celljumpTwice++;
				}
			}
		}

		//metrics.put(tagName + "_id",  new Long(this.));
		metrics.put(tagName + "_capacity",  new Long(currentCapacity));
		metrics.put(tagName + "_cardinality",  new Long(this.availableBits.cardinality()));
		metrics.put(tagName + "_uniqueids",  new Long(idsUnique.size()));
		metrics.put(tagName + "_latest",  new Long(latest));
		metrics.put(tagName + "_expired",  new Long(expired));
		metrics.put(tagName + "_cleanedup",  new Long(cleanup));
		metrics.put(tagName + "_celljumptotal",  new Long(celljump));
		metrics.put(tagName + "_celljumplatest",  new Long(celljumplatest));
		metrics.put(tagName + "_celljumpexpired",  new Long(celljumpexpired));
		metrics.put(tagName + "_celljumpOnce",  new Long(celljumpOnce));
		metrics.put(tagName + "_celljumpTwice",  new Long(celljumpTwice));
		metrics.put(tagName + "_invertedindexsize",  new Long(invertedIndex.size()));
	}

	public int getAvailableBitsCardinality() {
		return availableBits.cardinality();
	}

	public int getRecordHolderSize() {
		return recordHolders.length;
	}

	public BitSetExposed getAvailableBits() {
		return availableBits;
	}

	public Map<String, BitSetExposed> getInvertedIndex() {
		return invertedIndex;
	}

	public RecordHolder[] getRecordHolders() {
		return recordHolders;
	}
	
	public RecordHolder getRecordHolder(int pos) {
		if (pos < 0 || pos > (recordHolders.length - 1)) return null;
		return recordHolders[pos];
	}
	
	public Map<String, Object> getIndexDump(int pos) {
		if (pos < 0 || pos > (recordHolders.length - 1)) return null;
		
		Map<String, Object> indexData = new HashMap<>();
		indexData.put("available_bit", availableBits.get(pos));
		indexData.put("updated_timestamp", updatedTimestamp[pos]);
		
		for(Entry<String, BitSetExposed> entry : invertedIndex.entrySet()) {
			indexData.put("inverted_index_" + entry.getKey(), entry.getValue().get(pos));
		}
		
		for(Entry<String, BitSetExposed> entry : nullStrips.entrySet()) {
			indexData.put("null_strip_" + entry.getKey(), entry.getValue().get(pos));
		}
		
		getArrayIndexDump(pos, indexData);
		return indexData;
	}

	private void getArrayIndexDump(int pos, Map<String, Object> indexData) {
		for(Entry<String, short[]> entry : shortArrayIndex.entrySet()) {
			indexData.put("short_array_index_" + entry.getKey(), entry.getValue()[pos]);
		}
		
		for(Entry<String, int[]> entry : intArrayIndex.entrySet()) {
			indexData.put("int_array_index_" + entry.getKey(), entry.getValue()[pos]);
		}
		
		for(Entry<String, float[]> entry : floatArrayIndex.entrySet()) {
			indexData.put("float_array_index_" + entry.getKey(), entry.getValue()[pos]);
		}
		
		for(Entry<String, long[]> entry : longArrayIndex.entrySet()) {
			indexData.put("long_array_index_" + entry.getKey(), entry.getValue()[pos]);
		}
		
		for(Entry<String, double[]> entry : doubleArrayIndex.entrySet()) {
			indexData.put("double_array_index_" + entry.getKey(), entry.getValue()[pos]);
		}
		
		for(Entry<String, String[]> entry : StringArrayIndex.entrySet()) {
			indexData.put("string_array_index_" + entry.getKey(), entry.getValue()[pos]);
		}
	}
	
}
