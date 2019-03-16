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

import com.olacabs.roadrunner.api.GeoSpatialStore;
import com.olacabs.roadrunner.api.IIndexFactory;
import com.olacabs.roadrunner.api.Record;

public class InvertedIndexFactory implements IIndexFactory {

	Map<String, GeoSpatialStore> geoSpatialStores = new HashMap<>();
	
    public Record getRecord() {
        return new RecordImplVersion();
    }
    
    public GeoSpatialStore getStore(String storeName) {
    		return getStore(storeName, 1, 10000, 1, 10000);
    }
    
    public GeoSpatialStore getStore(String storeName, int partitionerActorParallelism, int partitionerActorQueueSize, 
			int recordActorParallelism, int recordActorQueueSize) {
    		return getStore(storeName, partitionerActorParallelism, partitionerActorQueueSize, recordActorParallelism, recordActorQueueSize, -1, -1);
    }
    
    public GeoSpatialStore getStore(String storeName, int partitionerActorParallelism, int partitionerActorQueueSize, 
			int recordActorParallelism, int recordActorQueueSize, long partitionBucketTtlInMillis, long recordTtlInMillis) {
	    
    	GeoSpatialStore geoSpatialStore = geoSpatialStores.get(storeName);
	    	if(geoSpatialStore == null) {
	    		geoSpatialStore = new GeoSpatialStoreImpl(partitionerActorParallelism, partitionerActorQueueSize,
	    				recordActorParallelism, recordActorQueueSize, partitionBucketTtlInMillis, recordTtlInMillis);
	    		geoSpatialStores.put(storeName, geoSpatialStore);
	    	}
	    	return geoSpatialStore;
    }

    @Override
    public IIndexFactory setPartitionBucketInitialCapacity(int capacity) {
        PartitionBucket.INITIAL_CAPACITY = capacity;
        return this;
    }

    @Override
    public IIndexFactory setPartitionBucketSubsequentIncrement(int capacity) {
        PartitionBucket.SUBSEQUENT_INCREMENT = capacity;
        return this;
    }

    @Override
    public IIndexFactory setPartitionBucketCooloffPeriodMilliSeconds(int coolOffPeriod) {
        PartitionBucket.COOL_OF_PERIOD = coolOffPeriod;
        return this;

    }

    @Override
    public IIndexFactory setCleanupRecordsInterval(int interval) {
        PartitionActorSystem.CLEANUP_RECORDS_INTERVAL = interval;
        return this;
    }

    @Override
    public IIndexFactory setCheckSumRecordsInterval(int interval) {
        PartitionActorSystem.CHECKSUM_RECORDS_INTERVAL = interval;
        return this;
    }
}
