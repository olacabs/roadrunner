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

public class PartitionBucketMessage {
    public static enum RecordType {
        SOFTDELETE, INSERTCOMMIT, TOUCH, INSERTDELETE, CLEANUP, CHECKSUM
    }

    public PartitionBucket partitionBucket;
    public RecordType type;
    public RecordHolder holder;
    public RecordImplVersion existingRecord;
    public RecordImplVersion receivedRecord;
    public RecordImplVersion flattenExistingRecord;
    public RecordImplVersion flattenReceivedRecord;

    public PartitionBucketMessage(PartitionBucket partitionBucket) {
        this.partitionBucket = partitionBucket;
    }

    public PartitionBucketMessage insertCommit(RecordHolder holder, RecordImplVersion receivedRecord, RecordImplVersion flattenReceivedRecord) {
        this.type = RecordType.INSERTCOMMIT;
        this.holder = holder;
        this.receivedRecord = receivedRecord;
        this.flattenReceivedRecord = flattenReceivedRecord;
        return this;
    }

    public PartitionBucketMessage insertDelete(RecordHolder holder, RecordImplVersion existingRecord, RecordImplVersion receivedRecord,
                                               RecordImplVersion flattenReceivedRecord) {
        this.type = RecordType.INSERTDELETE;
        this.holder = holder;
        this.existingRecord = existingRecord;
        this.receivedRecord = receivedRecord;
        this.flattenReceivedRecord = flattenReceivedRecord;
        return this;
    }

    public PartitionBucketMessage touch(RecordHolder holder, RecordImplVersion existingRecord, RecordImplVersion flattenExistingRecord,
                                        RecordImplVersion flattenReceivedRecord) {

        this.type = RecordType.TOUCH;
        this.holder = holder;
        this.existingRecord = existingRecord;
        this.flattenExistingRecord = flattenExistingRecord;
        this.flattenReceivedRecord = flattenReceivedRecord;
        return this;
    }

    public PartitionBucketMessage softDelete(RecordHolder holder, RecordImplVersion existingRecord) {
        this.type = RecordType.SOFTDELETE;
        this.existingRecord = existingRecord;
        this.holder = holder;
        return this;
    }

    public PartitionBucketMessage cleanup() {
        this.type = RecordType.CLEANUP;
        return this;
    }

    public PartitionBucketMessage checksum() {
        this.type = RecordType.CHECKSUM;
        return this;
    }

	@Override
	public String toString() {
		return "PartitionBucketMessage [partitionBucket=" + partitionBucket + ", type=" + type + ", holder=" + holder + ", existingRecord=" + existingRecord + ", receivedRecord="
		        + receivedRecord + ", flattenExistingRecord=" + flattenExistingRecord + ", flattenReceivedRecord=" + flattenReceivedRecord + "]";
	}
    
}
