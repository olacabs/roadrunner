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
package com.olacabs.roadrunner;

import java.util.AbstractMap;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import com.olacabs.roadrunner.api.*;
import com.olacabs.roadrunner.api.IndexableField.FieldDataType;

public class Usage {

    public static void main(String[] args) throws Exception{

    	PartitionerS2 partitionerS2 = new PartitionerS2();
		PartitionerS2.S2_CELL_INDEXING_LEVEL = 13;
	 	PartitionerS2.S2_CELL_POINT_CACHEKEY_LEVEL = 17;
	 	
    	GeoSpatialStore store = IndexFactory.getDefaultFactory().getStore(Usage.class.getSimpleName());
		int index = 0;
		IndexableField[] fields = new IndexableField[6];
		fields[index++] = new IndexableField<String>("status", false, false, FieldDataType.INT, null);
		fields[index++] = new IndexableField<String>("product", false, false, FieldDataType.ARRSTRING, null);
		fields[index++] = new IndexableField<String>("enabled", false, false, FieldDataType.BOOLEAN, null);
		fields[index++] = new IndexableField<String>("update_time", true, true, FieldDataType.LONG, null);
		fields[index++] = new IndexableField<String>("create_time", true, true, FieldDataType.LONG, null);
		fields[index++] = new IndexableField<String>("extn.rating", true, true, FieldDataType.DOUBLE, null);

		store.setSchema(new IndexableField<String>("id", true, true, FieldDataType.STRING, null), fields);
		
		long fixTime = System.currentTimeMillis();

		Record record1 = IndexFactory.getDefaultFactory().getRecord();
		String latFieldName = "lat";
		String lonFieldName = "lon";
		record1.setSpecialFieldNames(latFieldName, lonFieldName);

		record1.setId("123");
		record1.setField(latFieldName, 12.7);
		record1.setField(lonFieldName, 77.5);
		record1.setField("status", 1);
		record1.setField("product", Arrays.asList("car", "bike"));
		record1.setField("enabled", true);
		record1.setField("update_time", fixTime - 10000);

		Map<String, Object> extn1 = new HashMap<String, Object>();
		extn1.put("city", "bangalore");
		extn1.put("rating", 8.0);
		record1.setField("extn", extn1);

		Record record2 = IndexFactory.getDefaultFactory().getRecord();;
		record2.setId("124");
		record2.setField(latFieldName, 12.7);
		record2.setField(lonFieldName, 77.5);
		record2.setField("status", 55);
		record2.setField("product", Arrays.asList("car", "bike"));
		record2.setField("enabled", true);
		record2.setField("update_time", fixTime);

		Map<String, Object> vehicle2 = new HashMap<String, Object>();
		vehicle2.put("engine_type", "cng");
		Map<String, Object> extn2 = new HashMap<String, Object>();
		extn2.put("vehicle", vehicle2);
		extn2.put("city", "Bangalore");
		extn2.put("rating", 8.0);
		record2.setField("extn", extn2);

		
		Map<String, Object> extn3 = new HashMap<String, Object>();
		Record record3 = IndexFactory.getDefaultFactory().getRecord();
		record3.setId("125");

		record3.setField(latFieldName, 12.7);
		record3.setField(lonFieldName, 77.5);
		record3.setField("status", 1);
		record3.setField("product", Arrays.asList("bicycle", "truck"));
		record3.setField("enabled", true);
		record3.setField("update_time", fixTime);
		extn3.put("city", "bangalore");
		record3.setField("extn", extn3);

		
		Record record4 = IndexFactory.getDefaultFactory().getRecord();
		record4.setId("126");
		record4.setField(latFieldName, 12.7);
		record4.setField(lonFieldName, 77.5);
		record4.setField("status", 1);
		record4.setField("product", Arrays.asList("car", "bike", "bicycle"));
		record4.setField("enabled", true);
		record4.setField("update_time", fixTime);
		Map<String, Object> vehicle4 = new HashMap<String, Object>();
		vehicle4.put("engine_type", "petrol");
		Map<String, Object> extn4 = new HashMap<String, Object>();
		extn4.put("vehicle", vehicle4);
		extn4.put("city", "delhi");
		extn4.put("rating", 7.0);
		record4.setField("extn", extn4);
		
		Record record5 = IndexFactory.getDefaultFactory().getRecord();
		record5.setId("127");
		record5.setField("status", 1);
		record5.setField("product", Arrays.asList("car", "bike", "bicycle"));
		record5.setField("enabled", true);
		Map<String, Object> vehicle5 = new HashMap<String, Object>();
		vehicle5.put("engine_type", "petrol");
		Map<String, Object> extn5 = new HashMap<String, Object>();
		extn5.put("vehicle", vehicle5);
		extn5.put("city", "delhi");
		extn5.put("rating", 7.0);
		record5.setField("extn", extn5);

		store.upsert(partitionerS2.getPartition(record1), record1).upsert(partitionerS2.getPartition(record2), record2)
			.upsert(partitionerS2.getPartition(record3), record3).upsert(partitionerS2.getPartition(record4), record4)
			.upsert(partitionerS2.getPartition(record5), record5);
	
		Thread.sleep(5);

		long sleepTimeMillis = 10000;
		long s = System.currentTimeMillis();
		GeoSpatialSearcher localSearcher = store.getDefaultGeoSpatialSearcher();
		Thread.sleep(sleepTimeMillis);
		
		localSearcher.buildByIds(new String[] {"123", "125", "126", "127"});
		RecordIndexes indexes = localSearcher.whereString("id").in(new String[] {"123", "125", "126", "127"})
				.and(localSearcher.whereString("product").equals("car"))
				.and(localSearcher.whereBoolean("enabled").isTrue());
		
		List<Record> records = localSearcher.stream(indexes).collect(Collectors.toList());
		for(Record record : records) {
			System.out.println("Device id filter : " + record);
		}
		
		for(int i = 0; i < 1; i++) {

			localSearcher.setRadial(12.7, 77.5, 3000).build(
					PartitionerS2.getS2CellsCovering(12.7, 77.5, 3000));

			indexes = localSearcher.whereBoolean("enabled").isTrue()
			.and(localSearcher.whereInt("status").equalTo(new int[]{1,55}))
			.and(localSearcher.whereLong("update_time").range(fixTime - 10000, fixTime))
			.and(localSearcher.whereString("extn.city").equalsIgnoreCase("bangalore"))
			.and(localSearcher.whereDouble("extn.rating").greaterThanEqualTo(7.0))
			.and(localSearcher.whereString("product").equals("bike"))
			.and(localSearcher.whereString("extn.vehicle.engine_type").missing())
			;

			Map<String, List<GeoSpatialRecord>> groupedRecords = new HashMap<>();
			localSearcher.streamGeoSpatial(indexes, null, "product",
					GeoSpatialRecord.DISTANCE, 10, groupedRecords);
			System.out.println( "groupedRecords=" + groupedRecords.toString());

			//Checking o/p of GeoSpatialSearcher.streamGeospatial(RecordIndexes,List,String,Map)
			groupedRecords = new HashMap<>();
			localSearcher.streamGeoSpatial(indexes, null, "product", groupedRecords);
			System.out.println( "Ids=" +  groupedRecords.entrySet().stream().map(entry ->
					new AbstractMap.SimpleEntry<>(entry.getKey(), entry.getValue().stream().map(record ->
							record.record.getId()).collect(Collectors.toList()))).collect(Collectors.toList()));
		}
		long e = System.currentTimeMillis();
		System.out.println("Completed : " + (e-s-sleepTimeMillis));
		System.exit(1);
    }
}
