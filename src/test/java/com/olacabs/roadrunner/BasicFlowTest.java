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

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.olacabs.BitSetExposed;
import com.olacabs.roadrunner.api.GeoSpatialRecord;
import com.olacabs.roadrunner.api.GeoSpatialSearcher;
import com.olacabs.roadrunner.api.GeoSpatialStore;
import com.olacabs.roadrunner.api.IndexFactory;
import com.olacabs.roadrunner.api.IndexableField;
import com.olacabs.roadrunner.api.IndexableField.FieldDataType;
import com.olacabs.roadrunner.api.Partitioner;
import com.olacabs.roadrunner.api.PartitionerHash;
import com.olacabs.roadrunner.api.PartitionerS2;
import com.olacabs.roadrunner.api.Record;
import com.olacabs.roadrunner.api.RecordIndexes;

import junit.framework.TestCase;
import org.junit.Assert;

public class BasicFlowTest extends TestCase {

    public void testHappyFlowS2() throws Exception  {

    	PartitionerS2 partitionerS2 = new PartitionerS2();
        PartitionerS2.S2_CELL_INDEXING_LEVEL = 13;
        PartitionerS2.S2_CELL_POINT_CACHEKEY_LEVEL = 17;
        
     	GeoSpatialStore store = IndexFactory.getDefaultFactory().getStore("BasicFlowTest1");
		int index = 0;
		IndexableField[] fields = new IndexableField[6];
		fields[index++] = new IndexableField<String>("status", false, false, FieldDataType.INT, null);
		fields[index++] = new IndexableField<String>("product", false, false, FieldDataType.ARRSTRING, null);
		fields[index++] = new IndexableField<String>("flag", false, false, FieldDataType.BOOLEAN, null);
		fields[index++] = new IndexableField<String>("create_time", true, true, FieldDataType.LONG, null);
		fields[index++] = new IndexableField<String>("update_time", true, true, FieldDataType.LONG, null);
		fields[index++] = new IndexableField<String>("extn.rating", true, true, FieldDataType.DOUBLE, null);

		store.setSchema(new IndexableField<String>("id", true, true, FieldDataType.STRING, null), fields);
		
		long fixTime = System.currentTimeMillis();
		IndexFactory.getDefaultFactory().getRecord().setSpecialFieldNames("lat", "lon");

		Record record1 = buildRecord(fixTime, partitionerS2,
				"123", 10000, true, 7.0, true, 1,
				12.7, 77.5, new String[]{"bike", "car"}, "bangalore", false, null);

		Record record2 = buildRecord(fixTime, partitionerS2,
				"124", 89, true, 9.0, true, 55,
				12.7, 77.5, new String[]{"car", "flight"}, "Bangalore", true, "cng");

		Record record3 = buildRecord(fixTime, partitionerS2,
				"125", 88, false, -1.0,true, 1,
				12.7, 77.5, new String[]{"ship", "foot"}, "bangalore", false, null);

		Record record4 = buildRecord(fixTime, partitionerS2,
				"126", 87, true, 8.0,true, 1,
				28.657522, 77.123433, new String[]{"bike", "car", "ship"}, "delhi", true, "petrol");

		store.upsert(partitionerS2.getPartition(record1), record1).upsert(partitionerS2.getPartition(record2), record2)
			.upsert(partitionerS2.getPartition(record3), record3).upsert(partitionerS2.getPartition(record4), record4);
	
		Thread.sleep(10000);


		GeoSpatialSearcher localSearcher1 = store.getDefaultGeoSpatialSearcher();
		localSearcher1.setRadial(12.7, 77.5, 3000).build(
				PartitionerS2.getS2CellsCovering(12.7, 77.5, 3000));

		checkPartitionS2Results(fixTime, localSearcher1);

		store.upsert(partitionerS2.getPartition(record1), record1.cloneSelectAll().setField("status", 55));

		System.out.println("testHappyFlowS2 - Completed");

    }


    public void testAbsoluteDelete() throws Exception{

        PartitionerS2 partitionerS2 = new PartitionerS2();
        PartitionerS2.S2_CELL_INDEXING_LEVEL = 13;
        PartitionerS2.S2_CELL_POINT_CACHEKEY_LEVEL = 17;


        GeoSpatialStore store = IndexFactory.getDefaultFactory().getStore("BasicFlowTest1");

        int index = 0;
        IndexableField[] fields = new IndexableField[6];
        fields[index++] = new IndexableField<String>("status", false, false, FieldDataType.INT, null);
        fields[index++] = new IndexableField<String>("product", false, false, FieldDataType.ARRSTRING, null);
        fields[index++] = new IndexableField<String>("flag", false, false, FieldDataType.BOOLEAN, null);
        fields[index++] = new IndexableField<String>("create_time", true, true, FieldDataType.LONG, null);
        fields[index++] = new IndexableField<String>("update_time", true, true, FieldDataType.LONG, null);
        fields[index++] = new IndexableField<String>("extn.rating", true, true, FieldDataType.DOUBLE, null);

        store.setSchema(new IndexableField<String>("id", true, true, FieldDataType.STRING, null), fields);

        long fixTime = System.currentTimeMillis();
        IndexFactory.getDefaultFactory().getRecord().setSpecialFieldNames("lat", "lon");

        Record record1 = buildRecord(fixTime, partitionerS2,
                "123", 10000, true, 7.0, true, 1,
                12.7, 77.5, new String[]{"bike", "car"}, "bangalore", false, null);

        Record record2 = buildRecord(fixTime, partitionerS2,
                "124", 89, true, 9.0, true, 55,
                12.7, 77.5, new String[]{"car", "flight"}, "Bangalore", true, "cng");

        store.upsert(partitionerS2.getPartition(record1), record1).upsert(partitionerS2.getPartition(record2),record2);

        Thread.sleep(10000);

        store.absoluteDelete("123");

        record1 = store.get("123");

        Assert.assertNull(record1);

        record2 = store.get("124");

        Assert.assertNotNull(record2);

    }


    private void checkPartitionS2Results(long fixTime, GeoSpatialSearcher localSearcher) {
		assertEquals(3, testFoundRecords("Flag", localSearcher.whereBoolean("flag").isTrue().getIds()) );
		assertEquals(testFoundRecords("Status", localSearcher.whereInt("status").equalTo(new int[]{55}).getIds()), 1 );
		assertEquals(testFoundRecords("FixTime", localSearcher.whereLong("create_time").range(fixTime - 10000, fixTime).getIds()), 3 );
		assertEquals(testFoundRecords("City", localSearcher.whereString("extn.city").equals("bangalore").getIds()), 2 );
		assertEquals(testFoundRecords("CityCamel", localSearcher.whereString("extn.city").equals("Bangalore").getIds()), 1 );
		assertEquals(testFoundRecords("CityIgnoreCase", localSearcher.whereString("extn.city").equalsIgnoreCase("bangalore").getIds()), 3 );
		assertEquals(testFoundRecords("Missing", localSearcher.whereString("extn.engine.fuel_type").missing().getIds()), 2 );
		assertEquals(testFoundRecords("GreaterThan", localSearcher.whereDouble("extn.rating").greaterThan(7.0).getIds()), 1 );
		assertEquals(testFoundRecords("GreaterThanEqual", localSearcher.whereDouble("extn.rating").greaterThanEqualTo(7.0).getIds()), 2 );
		assertEquals(testFoundRecords("LessthanEqualTo", localSearcher.whereDouble("extn.rating").lessThanEqualTo(9.0).getIds()), 2 );
		assertEquals(testFoundRecords("Lessthan", localSearcher.whereDouble("extn.rating").lessThan(9.0).getIds()), 1 );

		RecordIndexes indexes = localSearcher.whereBoolean("flag").isTrue()
				.and(localSearcher.whereInt("status").equalTo(new int[]{1,55}))
				.and(localSearcher.whereLong("create_time").range(fixTime - 10000, fixTime))
				.and(localSearcher.whereString("extn.city").equalsIgnoreCase("bangalore"))
				.and(localSearcher.whereDouble("extn.rating").greaterThanEqualTo(7.0))
				.and(localSearcher.whereString("product").equals("car"))
				.and(localSearcher.whereString("extn.engine.fuel_type").missing());
		BitSetExposed[] x = localSearcher.getResultDocIds();

		Map<String, List<GeoSpatialRecord>> groupedRecords = new HashMap<>();
		localSearcher.streamGeoSpatial(indexes, null, "product",
				GeoSpatialRecord.DISTANCE, 10, groupedRecords);
		assertEquals(groupedRecords.size(), 2);
		assertEquals(groupedRecords.containsKey("car"), true);
		assertEquals(groupedRecords.containsKey("bike"), true);
	}


	public void testHappyFlowHash() throws Exception  {
		PartitionerS2 partitionerS2 = new PartitionerS2();
        PartitionerS2.S2_CELL_INDEXING_LEVEL = 13;
        PartitionerS2.S2_CELL_POINT_CACHEKEY_LEVEL = 17;
        
		GeoSpatialStore store = IndexFactory.getDefaultFactory().getStore(BasicFlowTest.class.getSimpleName());
		int index = 0;
		IndexableField[] fields = new IndexableField[6];
		fields[index++] = new IndexableField<String>("status", false, false, FieldDataType.INT, null);
		fields[index++] = new IndexableField<String>("product", false, false, FieldDataType.ARRSTRING, null);
		fields[index++] = new IndexableField<String>("flag", false, false, FieldDataType.BOOLEAN, null);
//		fields[index++] = new IndexableField<String>("extn.city", false, false, FieldDataType.STRING, null);
		fields[index++] = new IndexableField<String>("create_time", true, true, FieldDataType.LONG, null);
		fields[index++] = new IndexableField<String>("update_time", true, true, FieldDataType.LONG, null);
		fields[index++] = new IndexableField<String>("extn.rating", true, true, FieldDataType.DOUBLE, null);
//		fields[index++] = new IndexableField<String>("extn.engine.fuel_type", false, false, FieldDataType.STRING, null);

		store.setSchema(new IndexableField<String>("id", true, true, FieldDataType.STRING, null), fields);
		
		long fixTime = System.currentTimeMillis();
		PartitionerHash partitionerHash = new PartitionerHash(100);

		IndexFactory.getDefaultFactory().getRecord().setSpecialFieldNames("lat", "lon");

		Record record1 = buildRecord(fixTime, partitionerHash,
				"123", 10000, true, 7.0, true, 1,
				12.7, 77.5, new String[]{"bike", "car"}, "bangalore", false, null);
		
		Record record2 = buildRecord(fixTime, partitionerHash,
				"124", 89, true, 9.0, true, 55,
				12.7, 77.5, new String[]{"car", "flight"}, "Bangalore", true, "cng");
		
		Record record3 = buildRecord(fixTime, partitionerHash,
				"125", 88, false, -1.0,true, 1,
				12.7, 77.5, new String[]{"ship", "foot"}, "bangalore", false, null);
		
		Record record4 = buildRecord(fixTime, partitionerHash,
				"126", 87, true, 8.0,true, 1,
				28.657522, 77.123433, new String[]{"bike", "car", "ship"}, "delhi", true, "petrol");
		
		store.upsert(partitionerS2.getPartition(record1), record1).upsert(partitionerS2.getPartition(record2), record2)
			.upsert(partitionerS2.getPartition(record3), record3).upsert(partitionerS2.getPartition(record4), record4);

		Thread.sleep(2000);

		GeoSpatialSearcher localSearcher1 = store.getDefaultGeoSpatialSearcher();
		localSearcher1.setRadial(12.7, 77.5, 3000).build();
		checkHashPartitionResults(fixTime, localSearcher1);

		System.out.println("testHappyFlowHash - Completed");

	}

	private void checkHashPartitionResults(long fixTime, GeoSpatialSearcher localSearcher) {
		assertEquals(testFoundRecords("Flag", localSearcher.whereBoolean("flag").isTrue().getIds()), 4 );
		assertEquals(testFoundRecords("Status", localSearcher.whereInt("status").equalTo(new int[]{55}).getIds()), 1 );
		assertEquals(testFoundRecords("FixTime", localSearcher.whereLong("create_time").range(fixTime - 10000, fixTime).getIds()), 4 );
		assertEquals(testFoundRecords("City", localSearcher.whereString("extn.city").equals("bangalore").getIds()), 2 );
		assertEquals(testFoundRecords("CityCamel", localSearcher.whereString("extn.city").equals("Bangalore").getIds()), 1 );
		assertEquals(testFoundRecords("CityIgnoreCase", localSearcher.whereString("extn.city").equalsIgnoreCase("bangalore").getIds()), 3 );
		assertEquals(testFoundRecords("Missing", localSearcher.whereString("extn.engine.fuel_type").missing().getIds()), 2 );
		assertEquals(testFoundRecords("GreaterThan", localSearcher.whereDouble("extn.rating").greaterThan(7.0).getIds()), 2 );
		assertEquals(testFoundRecords("GreaterThanEqual", localSearcher.whereDouble("extn.rating").greaterThanEqualTo(7.0).getIds()), 3 );
		assertEquals(testFoundRecords("LessthanEqualTo", localSearcher.whereDouble("extn.rating").lessThanEqualTo(9.0).getIds()), 3 );
		assertEquals(testFoundRecords("Lessthan", localSearcher.whereDouble("extn.rating").lessThan(9.0).getIds()), 2 );

		RecordIndexes indexes = localSearcher.whereBoolean("flag").isTrue()
				.and(localSearcher.whereDouble("extn.rating").greaterThanEqualTo(1.0));
		BitSetExposed[] x = indexes.getIds();
		int found = testFoundRecords("AllRecords", x);
		assertEquals(found, 3);

		Map<String, List<GeoSpatialRecord>> groupedRecords = new HashMap<>();
		localSearcher.streamGeoSpatial(indexes, null, "product",
				"extn.city", 10, groupedRecords);
		assertEquals(groupedRecords.size(), 3);
		assertEquals(groupedRecords.containsKey("car"), true);
		assertEquals(groupedRecords.containsKey("bike"), true);

		for ( Map.Entry<String,List<GeoSpatialRecord>> entry :  groupedRecords.entrySet()) {
			if ( "car".equals(entry.getKey())) {
				StringBuilder sb = new StringBuilder();
				for ( GeoSpatialRecord r : entry.getValue()) {
					HashMap attr = (HashMap) r.record.get("extn");
					sb.append( attr.get("city"));
					sb.append(",");
				}
				assertEquals("Bangalore,bangalore," , sb.toString());
			}
		}


	}


	private static Record buildRecord(long fixTime, Partitioner partitionerS2,
									String docId, Integer fixTimeBackSlideMillis,
									Boolean isDriverScore, Double driverScore,
									Boolean flag, Integer status,
									Double lat, Double lon, String[] categories,
									String city, Boolean isFuelType, String fuelType) {

		Record record1 = IndexFactory.getDefaultFactory().getRecord();;
		record1.setId(docId).setLatField(lat).setLonField(lon);
		record1.setField("status", status);
		record1.setField("product", Arrays.asList(categories));
		record1.setField("flag", flag);
		record1.setField("create_time", fixTime - fixTimeBackSlideMillis);

		Map<String, Object> attributes1 = new HashMap<String, Object>();
		attributes1.put("city", city);
		if ( isDriverScore ) attributes1.put("rating", driverScore);
		record1.setField("extn", attributes1);

		if ( isFuelType ) {
			Map<String, Object> engine = new HashMap<String, Object>();
			engine.put("fuel_type", fuelType);
			attributes1.put("engine", engine);
		}

		return record1;
	}

	public static int testFoundRecords(String step, BitSetExposed[] allBits) {
		int totalRecords = 0;
		for (BitSetExposed bits :  allBits) {
			totalRecords += bits.cardinality();
		}
		return totalRecords;
	}
}
