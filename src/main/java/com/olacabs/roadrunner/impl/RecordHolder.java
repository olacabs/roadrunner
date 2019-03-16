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
import java.util.Map;

import com.olacabs.roadrunner.api.Record;

public class RecordHolder {

	public static final RecordHolder EMPTY = new RecordHolder();
	private String id = null;
	private RecordImplVersion newRecord = null;
	private RecordImplVersion oldRecord = null;

	public final Record get(final String partitionValue, final int pos) {
		Record record = null;
		if ( null != newRecord) record = newRecord.get(partitionValue, pos);
		if ( null != record) return record;
		if ( null != oldRecord) record = oldRecord.get(partitionValue, pos);
		return record;
	}

	public final boolean isEmpty() {
		return ( null == this.newRecord && null == this.oldRecord);
	}

	/**
	 * @param changedFields
	 * @return
	 */
	public final RecordImplVersion update(final RecordImplVersion changedFields) {
		RecordImplVersion temp = new RecordImplVersion();
		if(this.newRecord != null) {
			temp.putAll(this.newRecord);
			temp.setPartitionValue(this.newRecord.getPartitionValue());
		}
		
		temp.merge(changedFields);
		
		String changePartitionValue = changedFields.getPartitionValue();
		if ( changePartitionValue != null && changePartitionValue.length() > 0)  temp.setPartitionValue(changePartitionValue);
		
		this.setOldRecord(this.newRecord);
		return this.setNewRecord(temp);
	}

	protected RecordImplVersion getNewRecord() {
		return this.newRecord;
	}

	protected RecordImplVersion getOldRecord() {
		return this.oldRecord;
	}

	protected String getId() {
		return this.id;
	}

	protected RecordImplVersion setNewRecord(RecordImplVersion record) {
		this.newRecord = record;
		if ( null == this.id) this.id = record.getId();
		return this.newRecord;
	}

	private void setOldRecord(RecordImplVersion record) {
		this.oldRecord = record;
	}

	/* (non-Javadoc)
	 * @see java.lang.Object#toString()
	 */
	@Override
	public String toString() {
		return "RecordHolder [newRecord=" + newRecord + ", oldRecord=" + oldRecord + "]";
	}

}
