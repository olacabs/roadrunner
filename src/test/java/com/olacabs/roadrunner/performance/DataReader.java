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

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.function.Consumer;
import java.util.stream.Collector;
import java.util.stream.Collectors;

import com.olacabs.roadrunner.api.*;
import org.slf4j.Logger;

import com.newrelic.api.agent.Trace;
import com.olacabs.roadrunner.utils.LoggerFactory;

public class DataReader implements Runnable {

	public static final class IntCounter {
		int counter = 0;
	}
	
	private GeoSpatialStore store;
	
	private int radius;
	
	private static Random random = new Random();
	
	private Map<String, List<GeoSpatialRecord>> groupedRecords = new HashMap<>();
	
	private static final Logger logger = LoggerFactory.getLogger(DataReader.class);
	
	public DataReader(GeoSpatialStore store, int radius) {
		this.store = store;
		this.radius = radius;
	}

	public static double randomInRange(double min, double max) {
		double range = max - min;
		double scaled = random.nextDouble() * range;
		double shifted = scaled + min;
		return shifted; // == (rand.nextDouble() * (max-min)) + min;
	}
	
	@Override
	public void run() {
		long start = System.currentTimeMillis();
		long s = System.currentTimeMillis();
		long e = System.currentTimeMillis();
		final IntCounter size = new IntCounter();
		int count = 0;
		while(true) {
			Map<String, List<GeoSpatialRecord>> groupedRecordsP = getNearbyDevices(count);

			if(groupedRecordsP != null) {
				size.counter = 0;
				for ( List<GeoSpatialRecord> gpL : groupedRecordsP.values()) {
					gpL.stream().sorted().forEach(new Consumer<GeoSpatialRecord>() {
						@Override
						public final void accept(final GeoSpatialRecord geoSpatialRecord) {
							size.counter++;
						}
					});
				}
				count++;
				if(count % 1000000 == 0) {
					e = System.currentTimeMillis();
					double mem = Runtime.getRuntime().maxMemory() - Runtime.getRuntime().totalMemory() + Runtime.getRuntime().freeMemory();
					System.out.println(
							"Search time/record : " + ((e-s) /1000) + " micron,\tResultset # : " + size.counter + ",\tmemory: " + mem/1000000.0 + " mb"
									+ ", rps : " + ((count * 1000) / (e -start)));
					s = System.currentTimeMillis();
				}
			}
		}
	}

	@Trace(dispatcher = true)
	private Map<String, List<GeoSpatialRecord>> getNearbyDevices(int callCount) {
		Map<String, List<GeoSpatialRecord>> records = null;
		try {
			String city = PerfTest.cities[random.nextInt(PerfTest.cities.length)];
			double[] latLonRange = PerfTest.latLonMap.get(city);
			double latitude = randomInRange(latLonRange[0], latLonRange[2]);
			double longitude = randomInRange(latLonRange[1], latLonRange[3]);
			long currentTime = System.currentTimeMillis();

			GeoSpatialSearcher localSearcher = s2Radial(latitude, longitude);
			String[] astrfld1 = null;
			Boolean extnLvl2Boolfld2 = true;
			if(callCount % 2 == 0) {
				astrfld1 = new String[] {"foot", "bike", "car"};
				extnLvl2Boolfld2 = null;
			}
			RecordIndexes indexes = filterRecordIndexes(city, currentTime, astrfld1, extnLvl2Boolfld2, localSearcher);

			records = sortRecords(localSearcher, indexes);
		} catch(Exception e) {
			logger.error("Error in nearby devices : {}", e.getMessage(), e);
		}
		return records;
	}

	private Map<String, List<GeoSpatialRecord>> sortRecords(GeoSpatialSearcher localSearcher, RecordIndexes indexes) {
		groupedRecords.clear();
		return localSearcher.streamGeoSpatial(indexes, null, "extn.lvl1.astrfld1",
				GeoSpatialRecord.DISTANCE, 1000, groupedRecords);
	}
	
	private RecordIndexes filterRecordIndexes(String city, long currentTime, String[] astrfld1,
											  Boolean extnLvl2Boolfld2, GeoSpatialSearcher localSearcher) {

		RecordIndexes indexes = localSearcher.whereString("superid").equals("tenant001");
		localSearcher.setResultDocIds(indexes.getIds());
		indexes = localSearcher.whereLong("created_at").greaterThanEqualTo(currentTime - 300000);
		localSearcher.setResultDocIds(indexes.getIds());
		indexes = localSearcher.whereString("strfld2").in(new String[] {"1", "6"});
		localSearcher.setResultDocIds(indexes.getIds());
		indexes = localSearcher.whereString("extn.longfld8").equals("enabled");
		localSearcher.setResultDocIds(indexes.getIds());
		indexes = localSearcher.whereString("extn.strfld2").equalsIgnoreCase(city);
		localSearcher.setResultDocIds(indexes.getIds());

		if(astrfld1 != null) {
			indexes = localSearcher.whereString("extn.lvl1.astrfld1").in(astrfld1);
			localSearcher.setResultDocIds(indexes.getIds());
		}

		if(extnLvl2Boolfld2 != null) {
			if(extnLvl2Boolfld2) {
				indexes = localSearcher.whereBoolean("extn.lvl2.boolfld2").isTrue();
			} else {
				indexes = localSearcher.whereBoolean("extn.lvl2.boolfld2").isFalse();
			}
			localSearcher.setResultDocIds(indexes.getIds());
		}
		return indexes;
	}
	private GeoSpatialSearcher s2Radial(double latitude, double longitude) {
		return store.getDefaultGeoSpatialSearcher().setRadial(latitude, longitude, radius).build(
				PartitionerS2.getS2CellsCovering(latitude, longitude, radius)
		);
	}

	
	static <T> Collector<T,?,List<T>> toSortedList(Comparator<? super T> c) {
	    return Collectors.collectingAndThen(
	        Collectors.toCollection(ArrayList::new), l->{ l.sort(c); return l; } );
	}

}
