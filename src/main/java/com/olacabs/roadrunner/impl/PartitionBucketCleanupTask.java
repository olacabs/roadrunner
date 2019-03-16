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

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map.Entry;
import java.util.Random;
import java.util.TimeZone;
import java.util.TimerTask;


import com.olacabs.BitSetExposed;
import com.olacabs.roadrunner.monitor.RoadRunnerMetricFactory;
import com.olacabs.roadrunner.utils.Logger;
import com.olacabs.roadrunner.utils.LoggerFactory;

public class PartitionBucketCleanupTask extends TimerTask {
	
	private GeoSpatialStoreImpl geoStore;
	
	private long partitionBucketTtlInMillis; 
	
	private long recordTtlInMillis;
	
	private static RoadRunnerMetricFactory metrics = RoadRunnerMetricFactory.getInstance();
	
	static Logger logger = LoggerFactory.getLogger(PartitionBucketCleanupTask.class.getName());
	
	/**
	 * @param geoStore
	 */
	public PartitionBucketCleanupTask(GeoSpatialStoreImpl geoStore, long partitionBucketTtlInMillis, long recordTtlInMillis) {
		this.geoStore = geoStore;
		this.partitionBucketTtlInMillis = partitionBucketTtlInMillis;
		this.recordTtlInMillis = recordTtlInMillis;
	}

	@Override
	public void run() {	
		logger.info("running record timer task");
		long currentTime = System.currentTimeMillis();
		List<String> partitionBucketKeysToBeRemoved = new ArrayList<>();
		
		for(Entry<String, PartitionBucket> partitionBucketEntry : geoStore.getPartitionBucketEntries()) {
			PartitionBucket partitionBucket = partitionBucketEntry.getValue();
			removeOutdatedRecords(partitionBucket);
			
			if(partitionBucket.getAvailableBitsCardinality() > 0) continue;
			
			long lastTouchedTime = partitionBucket.getLastTouchedTime();
			if((currentTime - lastTouchedTime) > partitionBucketTtlInMillis) {
				String partitionBucketKey = partitionBucketEntry.getKey();
				logger.info("Deleting partition bucket key : {}, lastTouchedTime : {}", partitionBucketKey, lastTouchedTime);
				partitionBucketKeysToBeRemoved.add(partitionBucketKey);
			}
		}
		metrics.increment("partitionBucketDeletion", partitionBucketKeysToBeRemoved.size());
		for(String partitionBucketKey : partitionBucketKeysToBeRemoved) {
			this.geoStore.removePartitionBucket(partitionBucketKey, false);
		}
	}

	private void removeOutdatedRecords(PartitionBucket partitionBucket) {
		long currentTime = System.currentTimeMillis();
		BitSetExposed availableBitSet = partitionBucket.getAvailableBits();
		for (int pos = availableBitSet.nextSetBit(0); pos >= 0; pos = availableBitSet.nextSetBit(pos+1)) {
			if( (currentTime - partitionBucket.getUpdatedTimestamp(pos)) > recordTtlInMillis ) {
				RecordHolder recordHolder = partitionBucket.getRecordHolder(pos);
				
				if(recordHolder != null) {				
					RecordImplVersion record = recordHolder.getNewRecord();
					if ( record.getPOS() == pos) {
						try {
							
							this.geoStore.delete(partitionBucket, recordHolder, record) ;
						} catch (Exception ex) {
							logger.warn("Error while deleting IMEI: {}",record.getId(), ex);
						}
					}
				}			
			}
		}
	}
	
	
	public static final long nonSynchronizeTTLTime() {
		Date now = new Date();
		DateFormat formatNow = new SimpleDateFormat("HH");
		formatNow.setTimeZone(TimeZone.getTimeZone("IST"));
		String formattedNow = formatNow.format(new Date());

		Random randomess = new Random();
		int hourBetween = randomess.nextInt(5); //12 Midnight to 5 AM

		int hourNow = Integer.parseInt(formattedNow);
		int hourAfter = (24 - hourNow + hourBetween);
		int minuteAfter = randomess.nextInt(59);

		long newTime = now.getTime() + (hourAfter*60*60*1000) + (minuteAfter*60*1000);
		logger.info ( " TTL Timing:  {} " , new Date(newTime).toString());
		
		return newTime-System.currentTimeMillis();
	}
	
	public static final long interval() {
		
		return 24 * 60 * 60 * 1000;
	}
	
	
	


}
