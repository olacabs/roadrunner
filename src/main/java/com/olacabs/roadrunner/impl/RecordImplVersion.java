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

import java.util.EnumMap;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Hashtable;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListMap;

import com.olacabs.roadrunner.api.Record;
import com.olacabs.roadrunner.utils.Haversine;
import com.olacabs.roadrunner.utils.Logger;
import com.olacabs.roadrunner.utils.LoggerFactory;

final class RecordImplVersion extends HashMap<String, Object> implements Record {

	private static final String SPACE = ", ";
	private static final long serialVersionUID = 1L;
	public static String ID = "ID" ;
	public static String LAT = "LAT" ;
	public static String LON = "LON" ;

	
	public static String[] latTokens = new String[] {LAT};
	public static String[] lonTokens = new String[] {LON};
	public static String[] partLatTokens = new String[] {};
	public static String[] partLonTokens = new String[] {};

	private int position = -1;
	private String partitionValue = null;
	private double lat = Double.NaN;
	private double lon = Double.NaN;
	private double latCosine = Double.NaN;
	
	static Logger logger = LoggerFactory.getLogger(RecordImplVersion.class);
	
	public RecordImplVersion() {
		super();
	}

	@Override
	public final Record create() {
		return new RecordImplVersion();
	}

	@Override
	public Record setSpecialFieldNames(String latFieldName, String lonFieldName) {
		if(latFieldName == null || latFieldName.length() == 0 ||
				lonFieldName == null || lonFieldName.length() == 0 ) {
			throw new IllegalArgumentException("Lat / lon field names can't be empty / null");
		}
		LAT = latFieldName;
		LON = lonFieldName;

		latTokens = latFieldName.split("\\.");
		lonTokens = lonFieldName.split("\\.");
	
		int length = latTokens.length;
		if(length > 1) {
			partLatTokens = new String[length - 1];
			System.arraycopy(latTokens, 1, partLatTokens, 0, length - 1);
		}
		length = lonTokens.length;
		if(length > 1) {
			partLonTokens = new String[length - 1];
			System.arraycopy(lonTokens, 1, partLonTokens, 0, length - 1);
		}
		return this;
	}

	@Override
	public final Record setIDFieldName(final String idField) {
		if(idField == null || idField.length() == 0 || idField.contains(".")) {
			throw new IllegalArgumentException("Id field name can't be empty / null / contain dot");
		}
		
		ID = idField;
		return this;
	}


	public final Record setId(final String recordId) {
		super.put(ID, recordId);
		return this;
	}

	public final String getId() {
		Object idO = this.get(ID);
		if ( null == idO) {
			logger.error(1000, "Fields are not set properly, requested for {}, total keys {}, containskey {}" 
					+ " and contains record {}", ID, this.size(), this.containsKey(ID), this);
		}
		return (String) this.get(ID);
	}

	public final String getPartitionValue() {
		return this.partitionValue;
	}

	public final Record setPartitionValue(final String partitionValue) {
		this.partitionValue = partitionValue;
		return this;
	}

	public final int getPOS() {
		return this.position; 
	}

	public final void setPos(final int pos) {
		this.position = pos;
	}

	public Record setLatField(double lat) {
		this.put(LAT, lat);
		return this;
	}
	public Record setLonField(double lon) {
		this.put(LON, lon);
		return this;
	}

	public final double getLat() {
		return this.lat;
	}
	
	public final double getLon() { 
		return this.lon; 
	}

	public final double measureDistanceInMeters( double endLat, double endLon, double endLatCos) {
		return Haversine.distance(this.lat, this.lon, endLat, endLon, latCosine, endLatCos);
	}

	/**
	@Override
	public double getLatCosine() {
		return latCosine;
	}
	*/

	protected void setLat(double lat) {
		this.lat = lat;
	}

	protected void setLon(double lon) {
		this.lon = lon;
	}

	protected void setLatCosine(double latCosine) {
		this.latCosine = latCosine;
	}

	public final RecordImplVersion reset(final RecordImplVersion existingRecord) {
		this.partitionValue = existingRecord.partitionValue;
		this.position = existingRecord.position;
		this.lat = existingRecord.lat;
		this.lon = existingRecord.lon;
		this.latCosine = existingRecord.latCosine;

		this.clear();
		this.putAll(existingRecord);
		return  this;
	}

	public final Record setField(final String fldName, final Object val) {
		put(fldName, val);
		return this;
	}

	@Override
	public final Object put(final String fldName, final Object val) {
		
		if(latTokens[0].equals(fldName)) {
			if ( latTokens.length == 1 ) {
				this.lat = (Double) val;
				this.latCosine = Haversine.cosapprox(this.lat * Haversine.PIBY180);
			
			} else {
				this.lat = (Double) getTokenizedField(partLatTokens, (Map<String, Object>) val);
				this.latCosine = Haversine.cosapprox(this.lat * Haversine.PIBY180);
			}
		} 
		if ( lonTokens[0].equals(fldName)) {
			if ( lonTokens.length == 1 ) {
				this.lon = (Double) val;
			} else {
				this.lon = (Double) getTokenizedField(partLonTokens, (Map<String, Object>) val);
			}
		}
		return super.put(fldName, val);
	}
	
	@Override
	public final void putAll(final Map<? extends String, ? extends Object> m) {
		setLatLonFields(m);
		super.putAll(m);
	}

	private final void setLatLonFields(Map<? extends String, ? extends Object> m) {
		Object val = getTokenizedField(latTokens, m);
		if ( null != val ) {
			this.lat = (Double) val;
			this.latCosine = Haversine.cosapprox(this.lat * Haversine.PIBY180);
		}

		val = getTokenizedField(lonTokens, m);
		if ( null != val ) this.lon = (Double) val;
	}
	
	private final Object getTokenizedField(final String[] tokens, final Map<? extends String, ? extends Object> dataMap) {
		int length = tokens.length;
		if(length == 1) return dataMap.get(tokens[0]);

		Map<? extends String, ? extends Object> intermediateMap = dataMap;
		for(int i = 0; i < length - 1; i++) {
			intermediateMap = (Map<String, Object>) intermediateMap.get(tokens[i]);
			if(intermediateMap == null) {
				return null;
			}
		}
		return intermediateMap.get(tokens[length - 1]);
	}

	@Override
	public final void merge(final Map<? extends String, ? extends Object> m) {
		setLatLonFields(m);
		Set<String> keysToBeRemoved = mergeNestedMap(this, m);
		keysToBeRemoved.forEach(key -> m.remove(key));
		putAll(m);
	}
	
	

	private final Set<String> mergeNestedMap(Map<String, Object> lhs, Map<? extends String, ? extends Object> rhs) {
		Set<String> keysToBeRemoved = new HashSet<>();
		for(Map.Entry<? extends String, ? extends Object> entry : rhs.entrySet()) {
			String key = entry.getKey();
			Object val = entry.getValue();
			if(val != null && checkIfInstanceOfMap(val)) {
				merge(lhs, key, (Map<String, Object>) val);
				keysToBeRemoved.add(key);
			}
		}
		return keysToBeRemoved;
	}

	public final void merge(final Map<String, Object> lhsParent, String key, final Map<String, Object> rhs) {
		Map<String, Object> lhs = (Map<String, Object>) lhsParent.get(key);
		if(lhs == null) {
			lhsParent.put(key, rhs);
			return;
		}

		for(Entry<String, Object> rshData : rhs.entrySet()) {
			Object val = rshData.getValue();
			if(val != null && checkIfInstanceOfMap(val)) {
				merge(lhs, rshData.getKey(), (Map<String, Object>) val);

			} else {
				lhs.put(rshData.getKey(), val);
			}
		}
	}

	public final RecordImplVersion get(final String s2, final int pos) {
		if (pos == position && s2.equals(partitionValue)) return this;
		return null;
	}

	public final Object getField(final String fldName) {
		return this.get(fldName);
	}

	public final Object getTokenizedField(final String fldName) {
		if(fldName == null || fldName.length() == 0) return null;

		String[] tokens = fldName.split("\\.");
		return getTokenizedField(tokens);
	}

	public final Object getTokenizedField(final String[] tokens) {
		int length = tokens.length;
		if(length == 1) return this.get(tokens[0]);

		Map<String, Object> dataMap = this;
		for(int i = 0; i < length - 1; i++) {
			dataMap = (Map<String, Object>) dataMap.get(tokens[i]);
			if(dataMap == null) {
				return null;
			}
		}
		return dataMap.get(tokens[length - 1]);
	}

	public final String getStringField(final String fldName) {
		Object object = this.get(fldName);
		if(object == null) return null;
		return (String) object;
	}
	public final Boolean getBooleanField(final String fldName) {
		Object object = this.get(fldName);
		if(object == null) return null;
		return (Boolean) object;
	}
	public final Byte getByteField(final String fldName) {
		Object object = this.get(fldName);
		if(object == null) return null;
		return (Byte) object;
	}
	public final Short getShortField(final String fldName) {
		Object object = this.get(fldName);
		if(object == null) return null;
		return (Short) object;
	}
	public final Integer getIntegerField(final String fldName) {
		Object object = this.get(fldName);
		if(object == null) return null;
		return (Integer) object;
	}
	public final Float getFloatField(final String fldName) {
		Object object = this.get(fldName);
		if(object == null) return null;
		return (Float) object;
	}
	public final Long getLongField(final String fldName) {
		Object object = this.get(fldName);
		if(object == null) return null;
		return (Long) object;
	}

	public final Double getDoubleField(final String fldName) {
		Object object = this.get(fldName);
		if(object == null) return null;
		return (Double) object;
	}

	@Override
	public final short[] getShortArrayField(final String fldName) {
		Object object = this.get(fldName);
		if(object == null) {
			return null;
		}
		return (short[]) object;
	}
	@Override
	public final int[] getIntegerArrayField(final String fldName) {
		Object object = this.get(fldName);
		if(object == null) {
			return null;
		}
		return (int[]) object;
	}
	@Override
	public final float[] getFloatArrayField(final String fldName) {
		Object object = this.get(fldName);
		if(object == null) {
			return null;
		}
		return (float[]) object;
	}
	@Override	
	public final long[] getLongArrayField(final String fldName) {
		Object object = this.get(fldName);
		if(object == null) {
			return null;
		}
		return (long[]) object;
	}
	@Override
	public final double[] getDoubleArrayField(final String fldName) {
		Object object = this.get(fldName);
		if(object == null) {
			return null;
		}
		return (double[]) object;
	}

	@Override
	public final String[] getStringArrayField(final String fldName) {
		Object object = this.get(fldName);
		if(object == null) {
			return null;
		}
		return (String[]) object;
	}

	/* (non-Javadoc)
	 * @see java.lang.Object#toString()
	 */
	@Override
	public String toString() {
		StringBuilder sb = new StringBuilder();
		for ( Entry<String, Object> e : super.entrySet()) {
			sb.append(e.getKey()).append(':').append(String.valueOf(e.getValue())).append(SPACE);
		}
		sb.append("position").append(':').append(String.valueOf(position)).append(SPACE);
		sb.append("s2").append(':').append(String.valueOf(partitionValue)).append(SPACE);
		sb.append("lat").append(':').append(String.valueOf(lat)).append(SPACE);
		sb.append("lon").append(':').append(String.valueOf(lon)).append(SPACE);
		sb.append("latCosine").append(':').append(String.valueOf(latCosine));
		return sb.toString();
	}

}

