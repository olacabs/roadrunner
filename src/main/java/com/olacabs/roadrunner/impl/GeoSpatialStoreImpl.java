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
package com.olacabs.roadrunner.impl;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.Timer;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Stream;

import org.apache.commons.lang3.StringUtils;

import com.newrelic.api.agent.Trace;
import com.olacabs.roadrunner.api.GeoFence;
import com.olacabs.roadrunner.api.GeoSpatialSearcher;
import com.olacabs.roadrunner.api.GeoSpatialStore;
import com.olacabs.roadrunner.api.IRecordValidator;
import com.olacabs.roadrunner.api.IndexableField;
import com.olacabs.roadrunner.api.IndexableField.FieldDataType;
import com.olacabs.roadrunner.api.Record;
import com.olacabs.roadrunner.api.exceptions.RoadRunnerException;
import com.olacabs.roadrunner.monitor.RoadRunnerMetricFactory;
import com.olacabs.roadrunner.utils.Logger;
import com.olacabs.roadrunner.utils.LoggerFactory;

class GeoSpatialStoreImpl implements GeoSpatialStore {

	public static class InvalidField {
		boolean isValid = true;
		StringBuilder reason = null;

		public void makeInvalid() {
			this.isValid = false;
			if ( null == reason) reason = new StringBuilder();
		}
	}

	static Logger logger = LoggerFactory.getLogger(GeoSpatialStore.class.getName());
	
	public static final ThreadLocal<GeoSpacialSearcherImpl> searcherThreadLocal =
			new ThreadLocal<GeoSpacialSearcherImpl>() {
		@Override protected GeoSpacialSearcherImpl initialValue() {
			return new GeoSpacialSearcherImpl();
		}
	};
	
	private static RoadRunnerMetricFactory metrics = RoadRunnerMetricFactory.getInstance();
	
	private IRecordValidator recordValidator;
	
	IndexableField<?> primaryKey = null;

	Map<String, IndexableField<?>> columns = new ConcurrentHashMap<String, IndexableField<?>>();

	//    Key is device id
	Map<String, RecordHolder> records = new ConcurrentHashMap<String, RecordHolder>(1000000,10000);

	Map<String, PartitionBucket> partitionBuckets = new ConcurrentHashMap<String, PartitionBucket>(4096);
	DictionaryHolder dictionaryHolder = new DictionaryHolder();
	RecordFlattener recordFlattener = new RecordFlattener();

	PartitionActorSystem partitionBucketSystem;
	RecordActorSystem deviceSystem;

	private boolean geoFenceEnabled = false;
	private GeoFence[] geoFences = null;
	
	public long partitionBucketTtlInMillis = -1;
	
	protected GeoSpatialStoreImpl(int partitionerActorParallelism, int partitionerActorQueueSize, 
			int recordActorParallelism, int recordActorQueueSize, long partitionBucketTtlInMillis, long recordTtlInMillis) {
		
		this.partitionBucketSystem = new PartitionActorSystem(partitionerActorParallelism, partitionerActorQueueSize);
		this.deviceSystem = new RecordActorSystem(this, recordActorParallelism, recordActorQueueSize);
		logger.info("Starting up GeoSpatialStoreImpl");
		metrics.register(this);
		
		this.partitionBucketTtlInMillis = partitionBucketTtlInMillis;
		if(partitionBucketTtlInMillis > 0) {
			Timer PartitionBucketCleanupTimer = new Timer(true);
			PartitionBucketCleanupTask task = new PartitionBucketCleanupTask(this, partitionBucketTtlInMillis, recordTtlInMillis);
			PartitionBucketCleanupTimer.schedule(task,PartitionBucketCleanupTask.nonSynchronizeTTLTime(), PartitionBucketCleanupTask.interval());
		}
	}

	public GeoSpatialStore setSchema(IndexableField primaryKey, IndexableField[] fields) {
		this.primaryKey = primaryKey;
		for(IndexableField<?> field : fields) {
			validateIndexableField(field);
			String name = field.getName();
			columns.put(name, field);

			if(!field.isMostlyUnique()) {
				dictionaryHolder.createDictionary(field);
			}
		}
		recordFlattener.setIndexableFields(columns);

		RecordImplVersion.ID = primaryKey.getName();
		return this;
	}
	
	public GeoSpatialStore setRecordValidator(IRecordValidator recordValidator) {
		this.recordValidator = recordValidator;
		return this;
	}

	private void validateIndexableField(IndexableField<?> field) {
		if(field.isMostlyUnique()) {
			FieldDataType dataType = field.getDataType();

			if(dataType == FieldDataType.ARRSHORT || dataType == FieldDataType.ARRINT || dataType == FieldDataType.ARRFLOAT 
					|| dataType == FieldDataType.ARRLONG || dataType == FieldDataType.ARRDOUBLE || dataType == FieldDataType.ARRSTRING) {

				logger.error(1000, "Unsupported indexable field {}", field.getName());
				throw new UnsupportedOperationException("Array data type is not supported for mostly unique fields as of now. Field : " + field);
			}
		}
	}

	public GeoSpatialStore addIndexableField(IndexableField field) {
		String name = field.getName();
		columns.put(name, field);
		if(!field.isMostlyUnique()) {
			dictionaryHolder.createDictionary(field);
		}

		for(PartitionBucket partitionBucket : partitionBuckets.values()) {
			partitionBucket.addIndexableField(field);
		}
		return this;
	}

	public IndexableField getIndexableField(String fieldName) {
		return columns.get(fieldName);
	}
	
	public Record get(String recordId) {
		RecordHolder recordHolder = records.get(recordId);
		if(recordHolder == null) return null;
		return recordHolder.getNewRecord();
	}
	
	protected RecordHolder getRecordHolder(String recordId) {
		return records.get(recordId);
	}
	
	protected Set<Entry<String, PartitionBucket>> getPartitionBucketEntries() {
		return this.partitionBuckets.entrySet();
	}

	protected void removePartitionBucket(String partitionBucketKey, boolean hardDelete) {
		PartitionBucket partitionBucket = this.partitionBuckets.get(partitionBucketKey);
		if(hardDelete || (System.currentTimeMillis() - partitionBucket.getLastTouchedTime()) > partitionBucketTtlInMillis) {
			this.partitionBuckets.remove(partitionBucketKey);
		}
	}
	
	public GeoSpatialStore upsert(final String partition, final Record aDocument) throws Exception {
		aDocument.setPartitionValue(partition);
		return upsert(aDocument);
	}
	
	public GeoSpatialStore upsert(final Record aDocument) throws Exception {
		deviceSystem.put(aDocument.getId(), aDocument, false);
		return this;
	}
	
	public GeoSpatialStore upsertIfFree(final String partition, final Record aDocument) throws Exception {
		aDocument.setPartitionValue(partition);
		return upsertIfFree(aDocument);
	}

	public GeoSpatialStore upsertIfFree(final Record aDocument) throws Exception {
		deviceSystem.put(aDocument.getId(), aDocument, true);
		return this;
	}


	@Trace(dispatcher = true)
	protected GeoSpatialStore onReceiveUpsert(final Record aDocument) {
		
		boolean valid = true;
		if(recordValidator != null) {
			valid = recordValidator.validate(aDocument);
		}
		if(valid) {

			metrics.increment("upsert", 1);

			if ( geoFenceEnabled ) {
				double lat = aDocument.getLat();
				double lon = aDocument.getLon();
				if(! Double.isNaN(lat) && lat != 0.0 && ! Double.isNaN(lon) && lon != 0.0) {
					boolean inRange = inGeoFenceRange(lat, lon);
					if (!inRange) {
						metrics.increment("upsert_outside_geofence", 1);
						return this;
					}
				}
			}

			String partitionValue = aDocument.getPartitionValue();
			
			cacheRepeatedStringValues(aDocument);
			upsert(aDocument, partitionValue);

		} else {
			metrics.increment("invalidrecord", 1);
		}
		
		return this;
	}

	private void cacheRepeatedStringValues(Record record) {
		cacheNestedRepeatedStringValues("", record);
	}
	
	private void cacheNestedRepeatedStringValues(String propertyPrefix, Map<String, Object> aDocument) {
		if (StringUtils.isNotBlank(propertyPrefix)) {
			propertyPrefix = propertyPrefix + ".";
		}
		for (Entry<String, Object> entry : aDocument.entrySet()) {
			Object value = entry.getValue();
			String key = entry.getKey();
			if (value instanceof Map) {
				Map<String, Object> dataMap = (Map<String, Object>) value;
				cacheNestedRepeatedStringValues(propertyPrefix + key, dataMap);
			
			} else {
				IndexableField<?> field = columns.get(propertyPrefix + key);
				if ( field != null ) {
					if( ! field.isMostlyUnique()) {
						
						switch (field.getDataType()) {
						case STRING:
							String val = value == null ? null : value.toString().intern();
							aDocument.put(key, val);
							break;
							
						case ARRSTRING:
							List<String> dataList = value == null ? null : (List<String>) value;
							if(dataList != null) {
								for(int i = 0; i < dataList.size(); i++) {
									String valObj = dataList.get(i);
									String valStr = valObj == null ? null :  valObj.toString().intern();
									dataList.set(i, valStr);
								}
							}
							break;

						default:
							break;
						}
					}
				}
			}
		}
	}
	

	private boolean inGeoFenceRange(double lat, double lon) {
		boolean inRange = true;
		boolean inExcludeRange = false;
		boolean includeEncountered = false;
	
		for(GeoFence geoFence : geoFences) {
			double[] boundary = geoFence.getBoundary();
			switch (geoFence.getType()) {
			case INCLUDE:
				includeEncountered = true;
				inRange = lat >= boundary[0] && lon >= boundary[1] && lat <= boundary[2] && lon <= boundary[3];
				break;
				
			case EXCLUDE:
				inExcludeRange = lat >= boundary[0] && lon >= boundary[1] && lat <= boundary[2] && lon <= boundary[3];
				if(inExcludeRange) inRange = false;
				break;

			default:
				break;
			}
			
			if(inRange && includeEncountered) break;
			if(inExcludeRange) break;
		}
		return inRange;
	}

	private GeoSpatialStore upsert(Record receivedRecord, final String partitionValue)  {
		long startNano = System.nanoTime();
		boolean success = false;
		try {
			RecordImplVersion flattenedReceivedRecord = new RecordImplVersion();
			recordFlattener.flattenMap(receivedRecord, flattenedReceivedRecord);

			InvalidField validity = validateRecord(flattenedReceivedRecord);
			if (!validity.isValid) {
				metrics.increment("invaliddocument", 1);
				logger.error( "Record is not valid : {}, Received Record : {}", validity.reason, receivedRecord);
				throw new IllegalArgumentException("Record is invalid " + validity.reason);
			}

			recordFlattener.transformArrayDataType(flattenedReceivedRecord);
			addToDictionary(flattenedReceivedRecord);

			addToPartition(partitionValue, receivedRecord, flattenedReceivedRecord);
			success = true;

		} catch ( Exception ex) {
			logger.error(100, "Error while upsert", ex);
			success = false;
		} finally {
			long endNano = System.nanoTime();
			metrics.measureMethodCall("upsert_method", (endNano - startNano), success);
		}
		return this;
	}

	private void addToPartition(
			String newPartitionValue, Record receivedRecord, RecordImplVersion flattenedReceivedRecord) throws Exception  {

		//Fresh Record
		final RecordHolder recordHolder = records.get(receivedRecord.getId());
		if (recordHolder == null) {
			addFreshRecord(newPartitionValue, receivedRecord, flattenedReceivedRecord);
			return;

		} else {
			metrics.increment("existingrecord", 1);
			//Existing Record
			RecordImplVersion existingRecord = recordHolder.getNewRecord();
			String existingPartitionValue = existingRecord.getPartitionValue();

			//Existing Record has metadata only
			if(existingPartitionValue == null) {
				appendToExistingMeta(newPartitionValue, recordHolder, receivedRecord, existingRecord, flattenedReceivedRecord);
				return;
			}

			//Existing Record location and new record meta.
			if(newPartitionValue == null) {
				newPartitionValue = existingPartitionValue;
			}

			RecordImplVersion receivedRecordO = (RecordImplVersion) receivedRecord;
			receivedRecordO.setPartitionValue(newPartitionValue);

			//Fresh PartitionBucket
			PartitionBucket newPartitionBucket = partitionBuckets.get(newPartitionValue);
			if(newPartitionBucket == null) {
				newPartitionBucket = new PartitionBucket(columns, newPartitionValue);
				partitionBuckets.put(newPartitionValue, newPartitionBucket);
			}

			if(existingPartitionValue.equals(newPartitionValue)) {
				//Vehicle is in same Cell.
				boolean threadSafe = true;

				/**
				 * TODO: Check if the value is same, in that case it's safe to update.
				 */
				for(String key : flattenedReceivedRecord.keySet()) {
					IndexableField<?> field = columns.get(key);
					if ( field != null ) {
						if(!field.isThreadSafe()) {
							threadSafe = false;
							break;
						}
					}
				}

				if(threadSafe) {

					RecordImplVersion existingFlattenedRecord = new RecordImplVersion();
					recordFlattener.flattenMap(existingRecord, existingFlattenedRecord);
					recordFlattener.transformArrayDataType(existingFlattenedRecord);

					existingRecord.merge(receivedRecord);

					partitionBucketSystem.put(newPartitionValue, new PartitionBucketMessage(newPartitionBucket).touch(
							recordHolder, existingRecord, existingFlattenedRecord, flattenedReceivedRecord));
					metrics.increment("existingrecord_touch", 1);

				} else {

					/**
					 * 2 calls to the same cell and the first one has not inserted yet.
					 * So existing position will not be available. However, both will be executed in order.
					 */
					RecordImplVersion mergedRecord = recordHolder.update(receivedRecordO);
					RecordImplVersion flattenedMergedRecord = new RecordImplVersion();
					recordFlattener.flattenMap(mergedRecord, flattenedMergedRecord);
					recordFlattener.transformArrayDataType(flattenedMergedRecord);

					partitionBucketSystem.put(newPartitionValue,
							new PartitionBucketMessage(newPartitionBucket).insertDelete(
									recordHolder, existingRecord, mergedRecord, flattenedMergedRecord));


					metrics.increment("existingrecord_changed", 1);
				}
			} else {
				//Vehicle has moved out.

				RecordImplVersion mergedRecord = recordHolder.update(receivedRecordO);
				RecordImplVersion flattenedMergedRecord = new RecordImplVersion();
				recordFlattener.flattenMap(mergedRecord, flattenedMergedRecord);
				recordFlattener.transformArrayDataType(flattenedMergedRecord);

				partitionBucketSystem.put(newPartitionValue,
						new PartitionBucketMessage(newPartitionBucket).insertCommit(
								recordHolder, mergedRecord, flattenedMergedRecord));
				metrics.increment("existingrecord_crossed", 1);

				PartitionBucket existingPartitionBucket = partitionBuckets.get(existingPartitionValue);
				metrics.increment("existingrecord_softdelete", 1);
				partitionBucketSystem.put(existingPartitionValue, new PartitionBucketMessage(existingPartitionBucket).softDelete(recordHolder, existingRecord));

			}
		}
	}

	private void appendToExistingMeta(String newPartitionValue,
			RecordHolder recordHolder, Record receivedRecord, RecordImplVersion existingRecord,
			RecordImplVersion flattenedReceivedRecord) throws Exception {

		existingRecord.merge(receivedRecord);
		if(newPartitionValue == null) {
			metrics.increment("existingrecord_appendmeta", 1);
		} else {

			existingRecord.setPartitionValue(newPartitionValue);
			RecordImplVersion flattenedMergedRecord = new RecordImplVersion();
			recordFlattener.flattenMap(existingRecord, flattenedMergedRecord);
			recordFlattener.transformArrayDataType(flattenedMergedRecord);

			insertSingleVersion(receivedRecord.getId(), newPartitionValue, recordHolder, existingRecord, flattenedMergedRecord);
			metrics.increment("existingrecord_appendlocation", 1);
		}
	}

	private void addFreshRecord(
			String newPartitionValue, Record receivedRecord, RecordImplVersion flattenedReceivedRecord) throws Exception {

		metrics.increment("freshrecord", 1);
		RecordHolder recordHolder;
		if(newPartitionValue == null) {
			recordHolder = new RecordHolder();
			recordHolder.setNewRecord((RecordImplVersion) receivedRecord);

			String docId = receivedRecord.getId();
			metrics.increment("freshrecord_meta_attempt", 1);
			records.put(docId, recordHolder);
			metrics.increment("freshrecord_meta", 1);

		} else {
			//We have newPartitionValue here
			metrics.increment("freshrecord_location", 1);
			recordHolder = new RecordHolder();
			receivedRecord.setPartitionValue(newPartitionValue);
			insertSingleVersion(receivedRecord.getId(), newPartitionValue, recordHolder,
					recordHolder.setNewRecord((RecordImplVersion) receivedRecord), flattenedReceivedRecord);
		}
	}

	private void insertSingleVersion(String id, String newPartitionValue, RecordHolder recordHolder,
			RecordImplVersion receivedRecord,	RecordImplVersion flattenedReceivedRecord) throws Exception {

		metrics.increment("insertSingleVersion",1);
		PartitionBucket partitionBucket = partitionBuckets.get(newPartitionValue);
		if(partitionBucket == null) {
			partitionBucket = new PartitionBucket(columns, newPartitionValue);
			partitionBuckets.put(newPartitionValue, partitionBucket);
			metrics.increment("newPartitionBucket", 1);
		}

		//int insertPos = partitionBucket.insert(recordHolder);
		//partitionBucket.insertCommit(insertPos);
		partitionBucketSystem.put(newPartitionValue,
				new PartitionBucketMessage(partitionBucket).insertCommit(recordHolder, receivedRecord, flattenedReceivedRecord));
		records.put(id, recordHolder);
	}

	private InvalidField validateRecord(Record aDocument) {

		InvalidField validity = new InvalidField();

		for(IndexableField<?> field : columns.values()) {

			String fieldName = field.getName();
			Object object = aDocument.get(fieldName);
			if(object != null) {
				switch (field.getDataType()) {
				case BOOLEAN:
					if(object != null) {
						String objStr = String.valueOf(object);
						if("true".equalsIgnoreCase(objStr)) {
							object = true;
						} else if("false".equalsIgnoreCase(objStr)) {
							object = false;
						} else {
							validity.makeInvalid();
							validity.reason.append("\tInvalid BOOLEAN field : [" ).append(fieldName).append("] val=[").append(objStr).append(']');
						}
					}
					break;

				case SHORT:
					if ( ! (object instanceof Short) ) {
						validity.makeInvalid();
						validity.reason.append("\tInvalid SHORT field : [" ).append(fieldName).append("] val=[").append(object.getClass().toString()).append(']');
					}
					break;

				case INT:
					if ( ! (object instanceof Integer) ) {
						validity.makeInvalid();
						validity.reason.append("\tInvalid INT field : [" ).append(fieldName).append("] val=[").append(object.getClass().toString()).append(']');
					}
					break;

				case FLOAT:
					if ( ! (object instanceof Float) ) {
						validity.makeInvalid();
						validity.reason.append("\tInvalid FLOAT field : [" ).append(fieldName).append("] val=[").append(object.getClass().toString()).append(']');
					}
					break;

				case LONG:
					if ( ! (object instanceof Long) ) {
						validity.makeInvalid();
						validity.reason.append("\tInvalid LONG field : [" ).append(fieldName).append("] val=[").append(object.getClass().toString()).append(']');
					}
					break;

				case DOUBLE:
					if ( ! (object instanceof Double) ) {
						validity.makeInvalid();
						validity.reason.append("\tInvalid DOUBLE field : [" ).append(fieldName).append("] val=[").append(object.getClass().toString()).append(']');
					}
					break;

				case STRING:
					if ( ! (object instanceof String) ) {
						validity.makeInvalid();
						validity.reason.append("\tInvalid STRING field : [" ).append(fieldName).append("] val=[").append(object.getClass().toString()).append(']');
					}
					break;

				case ARRSHORT:
				{
					boolean validField = (object instanceof List);
					if(validField) {
						List<Object> dataList = (List<Object>) object;
						for(Object dataObj : dataList) {
							if(! (dataObj instanceof Short)) {
								validity.makeInvalid();
								validity.reason.append("\tInvalid ARRSHORT/ELEMENT field : [" ).append(fieldName).append("] val=[").append(object.getClass().toString()).append(']');
								break;
							}
						}
					} else {
						validity.makeInvalid();
						validity.reason.append("\tInvalid ARRSHORT field : [" ).append(fieldName).append("] val=[").append(object.getClass().toString()).append(']');
					}
				}
				break;

				case ARRINT:
				{
					boolean validField = (object instanceof List);
					if(validField) {
						List<Object> dataList = (List<Object>) object;
						for(Object dataObj : dataList) {
							if(! (dataObj instanceof Integer)) {
								validity.makeInvalid();
								validity.reason.append("\tInvalid ARRINT/ELEMENT field : [" ).append(fieldName).append("] val=[").append(object.getClass().toString()).append(']');
								break;
							}
						}
					} else {
						validity.makeInvalid();
						validity.reason.append("\tInvalid ARRINT field : [" ).append(fieldName).append("] val=[").append(object.getClass().toString()).append(']');
					}
				}
				break;

				case ARRFLOAT:
				{
					boolean validField = (object instanceof List);
					if(validField) {
						List<Object> dataList = (List<Object>) object;
						for(Object dataObj : dataList) {
							if(! (dataObj instanceof Float)) {
								validity.makeInvalid();
								validity.reason.append("\tInvalid ARRFLOAT/ELEMENT field : [" ).append(fieldName).append("] val=[").append(object.getClass().toString()).append(']');
								break;
							}
						}
					} else {
						validity.makeInvalid();
						validity.reason.append("\tInvalid ARRFLOAT field : [" ).append(fieldName).append("] val=[").append(object.getClass().toString()).append(']');
					}
				}
				break;

				case ARRLONG:
				{
					boolean validField = (object instanceof List);
					if(validField) {
						List<Object> dataList = (List<Object>) object;
						for(Object dataObj : dataList) {
							if(! (dataObj instanceof Long)) {
								validity.makeInvalid();
								validity.reason.append("\tInvalid ARRLONG/ELEMENT field : [" ).append(fieldName).append("] val=[").append(object.getClass().toString()).append(']');
								break;
							}
						}
					} else {
						validity.makeInvalid();
						validity.reason.append("\tInvalid ARRLONG field : [" ).append(fieldName).append("] val=[").append(object.getClass().toString()).append(']');
					}
				}
				break;

				case ARRDOUBLE:
				{
					boolean validField = (object instanceof List);
					if (validField) {
						List<Object> dataList = (List<Object>) object;
						for (Object dataObj : dataList) {
							if (!(dataObj instanceof Double)) {
								validity.makeInvalid();
								validity.reason.append("\tInvalid ARRDOUBLE/ELEMENT field : [" ).append(fieldName).append("] val=[").append(object.getClass().toString()).append(']');
								break;
							}
						}
					} else {
						validity.makeInvalid();
						validity.reason.append("\tInvalid ARRDOUBLE field : [").append(fieldName).append("] val=[").append(object.getClass().toString()).append(']');
					}
				}
				break;

				case ARRSTRING:
				{
					boolean validField = (object instanceof List);
					if(validField) {
						List<Object> dataList = (List<Object>) object;
						for(Object dataObj : dataList) {
							if(! (dataObj instanceof String)) {
								validity.makeInvalid();
								validity.reason.append("\tInvalid ARRSTRING/ELEMENT field : [" ).append(fieldName).append("] val=[").append(object.getClass().toString()).append(']');
								break;
							}
						}
					} else {
						validity.makeInvalid();
						validity.reason.append("\tInvalid ARRSTRING field : [").append(fieldName).append("] val=[").append(object.getClass().toString()).append(']');
					}
				}
				break;

				default:
					break;
				}
			}

			if ( ! validity.isValid) {
				logger.error(1000, "Error : {}", validity.reason.toString());
			}
		}
		return validity;
	}

	private void addToDictionary(Record aDocument) {
		for(String key : aDocument.keySet()) {
			IndexableField<?> field = columns.get(key);
			if ( field != null ) {
				if( ! field.isMostlyUnique()) {
					long start = System.nanoTime();
					addToDictionaryExploded(aDocument, key, field);
					metrics.measureMethodCall("addToDictionary", (System.nanoTime()-start), true);

				}
			}
		}
	}

	private void addToDictionaryExploded(Record aDocument, String key, IndexableField<?> field) {
		switch (field.getDataType()) {
		case SHORT:
			Short shortVal = aDocument.getShortField(key);
			if(shortVal != null) {
				dictionaryHolder.addShort(key, shortVal);
			}
			break;

		case INT:
			Integer intVal = aDocument.getIntegerField(key);
			if(intVal != null) {
				dictionaryHolder.addInteger(key, intVal);
			}
			break;

		case FLOAT:
			Float floatVal = aDocument.getFloatField(key);
			if(floatVal != null) {
				dictionaryHolder.addFloat(key, floatVal);
			}
			break;

		case DOUBLE:
			Double doubleVal = aDocument.getDoubleField(key);
			if(doubleVal != null) {
				dictionaryHolder.addDouble(key, doubleVal);
			}
			break;

		case LONG:
			Long longVal = aDocument.getLongField(key);
			if(longVal != null) {
				dictionaryHolder.addLong(key, longVal);
			}
			break;

		case STRING:
			String stringVal = aDocument.getStringField(key);
			if(stringVal != null) {
				dictionaryHolder.addString(key, stringVal);
			}
			break;

		case ARRSHORT:
		{
			short[] shortArray = aDocument.getShortArrayField(key);
			if(shortArray != null) {
				dictionaryHolder.addShort(key, shortArray);
			}
		}
		break;

		case ARRINT:
		{
			int[] intArray = aDocument.getIntegerArrayField(key);
			if(intArray != null) {
				dictionaryHolder.addInteger(key, intArray);
			}
		}
		break;
		case ARRFLOAT:
		{
			float[] floatArray = aDocument.getFloatArrayField(key);
			if(floatArray != null) {
				dictionaryHolder.addFloat(key, floatArray);
			}
		}
		break;
		case ARRLONG:
		{
			long[] longArray = aDocument.getLongArrayField(key);
			if(longArray != null) {
				dictionaryHolder.addLong(key, longArray);
			}
		}
		break;
		case ARRDOUBLE:
		{
			double[] doubleArray = aDocument.getDoubleArrayField(key);
			if(doubleArray != null) {
				dictionaryHolder.addDouble(key, doubleArray);
			}
		}
		break;
		case ARRSTRING:
		{
			String[] stringArray = aDocument.getStringArrayField(key);
			if(stringArray != null) {
				dictionaryHolder.addString(key, stringArray);
			}
		}
		break;

		default:
			break;
		}
	}

	public GeoSpatialStore delete(String recordId) throws Exception {
		RecordHolder recordHolder = records.get(recordId);
		String partitionBucketValue = (recordHolder != null) ? recordHolder.getNewRecord().getPartitionValue() : null;
		PartitionBucket partitionBucket = (partitionBucketValue != null) ? partitionBuckets.get(partitionBucketValue): null;

		if(partitionBucket != null) {
			partitionBucketSystem.put(partitionBucketValue, new PartitionBucketMessage(partitionBucket).softDelete(recordHolder, recordHolder.getNewRecord()));
		}

		return this;
	}

	@Override
	public GeoSpatialStore absoluteDelete(String recordId) throws Exception{

        this.delete(recordId);
        this.records.remove(recordId);

        return this;
    }
	
	public GeoSpatialStore delete(PartitionBucket partitionBucket, RecordHolder recordHolder, RecordImplVersion record ) throws Exception {
		if(record != null && partitionBucket != null) {
			
			partitionBucketSystem.put(partitionBucket.id, new PartitionBucketMessage(partitionBucket).softDelete(recordHolder, record));
		}
		return this;
	}

	public GeoSpatialStore delete(String[] recordIds) throws Exception  {
		for(String recordId : recordIds) {
			delete(recordId);
		}
		return this;
	}

	public GeoSpatialSearcher getDefaultGeoSpatialSearcher() {
		GeoSpacialSearcherImpl localSearcher = searcherThreadLocal.get();
		return localSearcher.setGeoSpatialStore(this).clear();
	}

	public GeoSpatialSearcher getGeoSpatialSearcher() {
		return new GeoSpacialSearcherImpl().setGeoSpatialStore(this);
	}

	@Override
	public Map<String, Long> collectMetrics() {

		int totalAvailable = 0;
		int totalRecordHolders = 0;
		int maxCardinality = Integer.MIN_VALUE;
		int minCardinality = Integer.MAX_VALUE;
		PartitionBucket lowestFillPartitionBucket = null;
		PartitionBucket highestFillPartitionBucket = null;
		int cleanup = 0;

		for ( PartitionBucket partitionBucket : partitionBuckets.values()) {
			int thisAvailable = partitionBucket.getAvailableBitsCardinality();
			totalRecordHolders += partitionBucket.getRecordHolderSize();
			totalAvailable += thisAvailable;
			cleanup += partitionBucket.cleanup;

			if (maxCardinality < thisAvailable) {
				maxCardinality = thisAvailable;
				highestFillPartitionBucket = partitionBucket;
			}
			if (minCardinality > thisAvailable) {
				minCardinality = thisAvailable;
				lowestFillPartitionBucket = partitionBucket;
			}
		}

		HashMap<String, Long > metrics = new HashMap<>(128);

		metrics.put("columns", new Long(columns.size()));
		metrics.put("records", new Long(records.size()));
		metrics.put("cells", new Long( partitionBuckets.size()));

		metrics.put("recordversions", new Long( totalAvailable) );
		metrics.put("recordholders", new Long(totalRecordHolders));
		metrics.put("s2maxrecords", new Long( maxCardinality) );
		metrics.put("s2minrecords", new Long( minCardinality) );
		if ( null != lowestFillPartitionBucket) lowestFillPartitionBucket.instrument(metrics, "lowestFillPartitionBucket");
		if ( null != highestFillPartitionBucket) highestFillPartitionBucket.instrument(metrics, "highestFillPartitionBucket");

		return metrics;
	}

	@Override
	public Map<String, Object> recordDump(String recordId) {
		Map<String, Object> indexData = new HashMap<>(8);
		RecordHolder recordHolder = records.get(recordId);
		if( null == recordHolder) return indexData;

		RecordImplVersion newRecord = (RecordImplVersion) recordHolder.getNewRecord();
		if(newRecord != null) {
			int newRecordPos = newRecord.getPOS();
			String newPartitionValue = newRecord.getPartitionValue();
			if(newPartitionValue != null && newRecordPos >= 0) {
				PartitionBucket partitionBucket = partitionBuckets.get(newPartitionValue);
				indexData.put("new_record_pos", newRecordPos);
				indexData.put("new_record_s2", newPartitionValue);
				if(partitionBucket!=null){
				indexData.put("new_record_index_dump", partitionBucket.getIndexDump(newRecordPos));
				indexData.put("new_record", partitionBucket.getRecordHolder(newRecordPos).getNewRecord());
				}
			} 
		}

		RecordImplVersion oldRecord = (RecordImplVersion) recordHolder.getOldRecord();
		if(oldRecord != null) {
			int oldRecordPos = oldRecord.getPOS();
			String oldPartitionValue = oldRecord.getPartitionValue();
			if(oldPartitionValue != null && oldRecordPos >= 0) {
				PartitionBucket partitionBucket = partitionBuckets.get(oldPartitionValue);
				indexData.put("old_record_pos", oldRecordPos);
				indexData.put("old_record_s2", oldPartitionValue);
				if(partitionBucket!=null){
				indexData.put("old_record_index_dump", partitionBucket.getIndexDump(oldRecordPos));
				indexData.put("old_record", partitionBucket.getRecordHolder(oldRecordPos).getOldRecord());
				}
			} 
		}
		return indexData;
	}

	@Override
	public Stream<Record> getRecords() {
		return records.values().stream().map(recordHolder -> recordHolder.getNewRecord());
	}

	public void setGeoFenceEnabled(boolean geoFenceEnabled) {
		if(geoFenceEnabled && (this.geoFences == null || this.geoFences.length == 0)) {
			logger.error("Please set geo fence first");
			throw new RoadRunnerException("Please set geo fence first");
		}
		this.geoFenceEnabled = geoFenceEnabled;
	}

	public void setGeoFences(GeoFence[] geoFences) {
		this.geoFences = geoFences;
	}
}
