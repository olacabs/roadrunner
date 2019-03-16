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
package com.olacabs.roadrunner.performance;

import java.util.HashMap;
import java.util.Map;

import com.olacabs.roadrunner.api.GeoSpatialStore;
import com.olacabs.roadrunner.api.IIndexFactory;
import com.olacabs.roadrunner.api.IndexFactory;
import com.olacabs.roadrunner.api.IndexableField;
import com.olacabs.roadrunner.api.IndexableField.FieldDataType;
import com.olacabs.roadrunner.api.PartitionerS2;
import com.olacabs.roadrunner.api.Record;
import com.olacabs.roadrunner.monitor.RoadRunnerMetricFactory;
import com.olacabs.roadrunner.monitor.RoadrunnerMetricLocal;

public class PerfTest {
	
	static String[] cities = new String[] {"bangalore"};
//	static String[] cities = new String[] {"bangalore", "mumbai", "hyderabad", "chennai", "delhi"};
	
	static Map<String, double[]> latLonMap = new HashMap<>();
    
    static {
//    	latLonMap.put("bangalore", new double[]{12.771625, 77.361946, 13.245794, 77.887917});
    	latLonMap.put("bangalore", new double[]{12.771625, 77.361946, 12.781625, 77.371946});
    	latLonMap.put("mumbai", new double[]{18.892, 72.744, 19.49636, 73.17511});
    	latLonMap.put("hyderabad", new double[]{17.16898, 78.065665, 17.610585, 78.685804});
    	latLonMap.put("chennai", new double[]{12.7104715, 79.9787093, 13.207693499, 80.320015});
    	latLonMap.put("delhi", new double[]{28.292664, 76.886324, 28.866076, 77.596999});
    }
	
	public static void main(String[] args) throws Exception {
		RoadRunnerMetricFactory.getInstance().register(new RoadrunnerMetricLocal());
		
		PartitionerS2 partitionerS2 = new PartitionerS2();
		PartitionerS2.S2_CELL_INDEXING_LEVEL = 13;
		PartitionerS2.S2_CELL_POINT_CACHEKEY_LEVEL = 17;
		
		IIndexFactory indexFactory = IndexFactory.getDefaultFactory();
		Record recordInstance = indexFactory.getRecord();
		recordInstance.setSpecialFieldNames("location.lat", "location.lon");
		recordInstance.setIDFieldName("id");
		
		GeoSpatialStore store = indexFactory.setCleanupRecordsInterval(5000).setCheckSumRecordsInterval(50000)
				.setPartitionBucketInitialCapacity(32).setPartitionBucketSubsequentIncrement(96)
				.setPartitionBucketCooloffPeriodMilliSeconds(5000).
				getStore(PerfTest.class.getSimpleName(), 2, 10000, 2, 10000);

		IndexableField[] fields = new IndexableField[21];
		fields[0] = new IndexableField<String>("superid", false, true, FieldDataType.STRING, null);
		fields[1] = new IndexableField<String>("created_at", true, true, FieldDataType.LONG, null);
		fields[2] = new IndexableField<String>("astrfld1", false, false, FieldDataType.ARRSTRING, null);
		fields[3] = new IndexableField<String>("strfld2", false, false, FieldDataType.STRING, null);

		fields[4] = new IndexableField<String>("extn.strfld1", false, false, FieldDataType.STRING, null);
		fields[5] = new IndexableField<String>("extn.strfld2", false, false, FieldDataType.STRING, null);
		fields[6] = new IndexableField<String>("extn.boolfld3", false, false, FieldDataType.BOOLEAN, null);
		fields[12] = new IndexableField<String>("extn.boolfld4", false, false, FieldDataType.BOOLEAN, null);
		fields[7] = new IndexableField<String>("extn.longfld5", true, true, FieldDataType.LONG, null);
		fields[15] = new IndexableField<String>("extn.astrfld6", false, false, FieldDataType.ARRSTRING, null);
		fields[16] = new IndexableField<String>("extn.astrfld7", false, false, FieldDataType.ARRSTRING, null);
		fields[17] = new IndexableField<String>("extn.longfld8", true, false, FieldDataType.LONG, null);
		fields[18] = new IndexableField<String>("extn.astrfld9", false, false, FieldDataType.ARRSTRING, null);
		fields[19] = new IndexableField<String>("extn.astrfld10", false, false, FieldDataType.ARRSTRING, null);
		fields[20] = new IndexableField<String>("extn.longfld11", true, false, FieldDataType.LONG, null);

		fields[8] = new IndexableField<String>("extn.lvl1.astrfld1", false, false, FieldDataType.ARRSTRING, null);
		fields[9] = new IndexableField<String>("extn.lvl2.boolfld2", false, false, FieldDataType.BOOLEAN, null);
		fields[10] = new IndexableField<String>("extn.lvl2.strfld3", false, false, FieldDataType.STRING, null);
		fields[11] = new IndexableField<String>("extn.lvl2.strfld4", false, false, FieldDataType.STRING, null);

		fields[13] = new IndexableField<String>("extn.lvl3.strfld1", false, false, FieldDataType.STRING, null);
		fields[14] = new IndexableField<String>("extn.lvl3.dblfld2", true, true, FieldDataType.DOUBLE, null);

		store.setSchema(new IndexableField<String>("id", true, true, FieldDataType.STRING, null), fields);
		
		int noOfWriteThreads = cities.length;

		Thread.sleep(20000);
		for(int i = 0; i < noOfWriteThreads; i++) {
			String city = cities[i];
			DataWriter writer = new DataWriter(store, partitionerS2, 100000, city, 10);
			//if ( 1 == 1 ) break;

			Thread thread = new Thread(writer);
			thread.start();
		}
		
		Thread.sleep(10000);

		if ( args.length == 0) args = new String[] {"1", "3000"};
		int noOfReadThreads = Integer.valueOf(String.valueOf(args[0]));
		int radius = Integer.valueOf(String.valueOf(args[1]));

		for(int i = 0; i < noOfReadThreads; i++) {
			DataReader reader = new DataReader(store, radius);

			Thread thread = new Thread(reader);
			thread.start();
		}

	}

}
