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
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ExecutorService;

import com.olacabs.roadrunner.api.GeoSpatialStore;
import com.olacabs.roadrunner.api.IndexFactory;
import com.olacabs.roadrunner.api.Partitioner;
import com.olacabs.roadrunner.api.Record;
import com.olacabs.roadrunner.monitor.RoadRunnerMetricFactory;

public class DataWriter implements Runnable {

    private int noOfDevices;

    private String city;

    private int moveByMeters;

    double R = 6371000; // meters , earth Radius approx

    double PI = 3.1415926535;

    double r_earth = 6378;

    double RADIANS = PI / 180;

    double DEGREES = 180 / PI;

    double bearing = 9.71;

    private Map<String, String> deviceLatLon = new HashMap<String, String>();

    ExecutorService writeExecutor;

    private static Random random = new Random();

    private GeoSpatialStore store;
    
    private Partitioner partitioner;
    
    private double latStart = 0.0;
    private double latEnd = 0.0;
    private double lonStart = 0.0;
    private double lonEnd = 0.0;
    
    private final static String SEPARATOR = "_";
    
	public static double randomInRange(double min, double max) {
		double range = max - min;
		double scaled = random.nextDouble() * range;
		double shifted = scaled + min;
		return shifted; // == (rand.nextDouble() * (max-min)) + min;
	}

    int range100 = 50;
    int range100000 = 100000;

    public void initializeLatLon(int noOfDevices, String city) {
    	this.noOfDevices = noOfDevices;
    	this.city = city;
        
    	this.latStart = PerfTest.latLonMap.get(city)[0];
    	this.latEnd = PerfTest.latLonMap.get(city)[2];
    	this.lonStart = PerfTest.latLonMap.get(city)[1];
    	this.lonEnd = PerfTest.latLonMap.get(city)[3];
        
    	for (int i = 0; i < noOfDevices; i++) {
    		String deviceId =  String.valueOf(i) ;
    		Double latitude = randomInRange(latStart, latEnd);
    		Double longitude = randomInRange(lonStart, lonEnd);
    		Double bearing = randomInRange(0.0, 360.0);

    		String value = latitude + SEPARATOR + longitude + SEPARATOR + bearing;
    		deviceLatLon.put(deviceId, value);
    	}
    }

    public DataWriter(GeoSpatialStore store, Partitioner partitioner, int noOfDevices, String city, int moveByMeters) {
    		initializeLatLon(noOfDevices, city);
        this.store = store;
        this.partitioner = partitioner;
        this.moveByMeters = moveByMeters;
    }

    public void moveXMeters(String deviceId) {

        String values = deviceLatLon.get(deviceId);
        String[] valueArray = values.split(SEPARATOR);
        double latitude = Double.valueOf(valueArray[0]) * RADIANS;
        double longitude = Double.valueOf(valueArray[1]) * RADIANS;
        double bearing = Double.valueOf(valueArray[2]);

        double distance = moveByMeters / R;
        double radbear = bearing * RADIANS;

        double lat2 = Math.asin(Math.sin(latitude) * Math.cos(distance) +

                Math.cos(latitude) * Math.sin(distance) * Math.cos(radbear));

        double dlon = Math.atan2(Math.sin(radbear) * Math.sin(distance) * Math.cos(latitude),

                Math.cos(distance) - Math.sin(latitude) * Math.sin(lat2));
        
        double lon2 = ((longitude + dlon + Math.PI) % (Math.PI * 2)) - Math.PI;
        
        double newLatitude = lat2 * DEGREES;
        double newLongitude = lon2 * DEGREES;
        
        if(newLatitude < latStart || newLatitude > latEnd || newLongitude < lonStart || newLongitude > lonEnd) {
        	newLatitude = randomInRange(latStart, latEnd);
        	newLongitude = randomInRange(lonStart, lonEnd);
			bearing = randomInRange(0.0, 360.0);
        }

        String newValue = newLatitude + SEPARATOR + newLongitude + SEPARATOR + bearing;
        deviceLatLon.put(deviceId, newValue);

    }

	public Record fillRecordLocation(Record record, double lat, double lng, String recordId) {
		record.setId(recordId);
		record.setField("id", recordId);
		record.setField("superid", "tenant001");
		record.setField("created_at", System.currentTimeMillis());
		record.setField("extn.longfld5", System.currentTimeMillis());
		record.setField("nonindexed_longFld1", System.currentTimeMillis());

		/**
		 * TODO:// Are they required here as we are declaring again down.
		 */
		record.put("location.lat", lat);
		record.put("location.lon", lng);

		Map<String, Double> location = new HashMap<>();
		location.put("lat", lat);
		location.put("lon", lng);
		record.put("location", location);
		return record;

	}

	public Record fillRecordMetadata(Record record, double lat, double lng,
			String recordId, String strFld2, double cfld1, String strfld1, String strfld2) {

		record.setId(recordId);
		record.setField("id", recordId);
		record.setField("superid", "tenant002");
		record.setField("created_at", System.currentTimeMillis());
		record.setField("extn.longfld5", System.currentTimeMillis());
		record.setField("nonindexed_longFld1", System.currentTimeMillis());

		List<String> products = Arrays.asList("foot", "bike", "car");
		record.setField("astrfld1", products);

		record.setField("strfld2", strFld2);
		record.setField("nonindexed_strFld2", "mobile");
		record.setField("nonindexed_intFld3", 7);
		record.setField("nonindexed_intFld4", 5);
		record.setField("nonindexed_floatFld5", 807.5876759330488);
		record.setField("nonindexed_intFld6", 12);
		record.setField("nonindexed_intFld7", 4);
		record.setField("nonindexed_intFld8", null);
		
		Map<String, Double> snappedLocation = new HashMap<>();
		snappedLocation.put("lat", lat);
		snappedLocation.put("lon", lng);
		record.put("nonindexed_mapFld8", snappedLocation);

		record.put("location.lat", lat);
		record.put("location.lon", lng);
		record.put("location", snappedLocation);
		
		Map<String, Double> location = new HashMap<>();
		location.put("lat", 0d);
		location.put("lon", 0d);
		record.put("nonindexed_mapFld9", location);
		record.put("nonindexed_mapFld10", location);

		Map<String, Object> extn = new HashMap<>();
		extn.put("id1", "");
		extn.put("xfld2", "wifi");
		extn.put("strfld1", strfld1);
		extn.put("strfld2", strfld2);
		extn.put("xfld3", 84205);
		extn.put("xfld4", 01);
		extn.put("xfld5", null);
		extn.put("xfld6", "");
		extn.put("xfld7", "car");
		extn.put("xfld8", "NA");
		extn.put("xfld9", "283752527823");
		extn.put("xfld10", 1503430297192L);
		extn.put("xfld11", "source-abc");
		extn.put("xfld12", false);
		extn.put("xfld13", "2017-08-21 17:14:03");
		extn.put("xfld14", "bangalore");
		extn.put("xfld15", null);
		extn.put("xfld16", "Timbuktu");
		extn.put("xfld17", null);
		extn.put("xfld18", null);
		extn.put("xfld19", null);
		extn.put("astrfld6", new ArrayList<>());
		extn.put("astrfld7",  new ArrayList<>());
		extn.put("xfld20", "NA");
		extn.put("xfld21", null);
		extn.put("xfld22", null);
		extn.put("superid", "tenant003");
		extn.put("created_at", 1502695123105L);
		extn.put("xfld23", 10558);
		extn.put("astrfld10", null);
		extn.put("longfld11", null);
		extn.put("astrfld9", null);
		extn.put("boolfld3", false);

		Map<String, Object> lvlA = new HashMap<>();
		lvlA.put("afld1", "none");
		lvlA.put("afld2", "activated");
		lvlA.put("afld3", "001");
		lvlA.put("afld4", "diesel");
		lvlA.put("afld5", true);
		lvlA.put("afld5", new String[] {"VAL1", "VAL2" , "VAL3" , "VAL4"});
		lvlA.put("afld6", new String[]{});
		lvlA.put("afld7", "VALUEOFFLD7");
		lvlA.put("afld8", null);
		lvlA.put("afld9", 14);
		lvlA.put("afld10", null);
		extn.put("lvlA", lvlA);
		
		Map<String, Object> lvlB = new HashMap<>();
		lvlB.put("bfld1", products);
		extn.put("lvlB", lvlB);

		Map<String, Object> lvlC = new HashMap<>();
		lvlC.put("cfld1",cfld1);
		lvlC.put("cfld2", "activated");
		lvlC.put("cfld3", "002");
		lvlC.put("cfld4", new String[]{});
		lvlC.put("cfld5", null);
		lvlC.put("cfld6", null);
		lvlC.put("cfld7", 147);
		extn.put("lvlC", lvlC);

		Map<String, Object> lvlD = new HashMap<>();
		lvlD.put("dfld1", 1501083970079L);
		extn.put("lvlD", lvlD);
		
		Map<String, Object> lvlE = new HashMap<>();
		lvlE.put("dfld1", null);
		lvlE.put("dfld2", null);
		lvlE.put("dfld3", null);
		lvlE.put("dfld4", null);
		lvlE.put("dfld5", null);
		extn.put("dfld6", lvlE);
		
		record.put("extn", extn);

		return record;
	}

	public void run() {
    	int rps = 20000;
    	int msTimeFor1000Records = (1000 * 1000 / rps);
    	long s = System.currentTimeMillis();
    	int records = 0;
    	long timeToSleep = 5000;
    	long remainingTime = 0;


    	int loop = -1;
    	while (true) {

			try {
				loop++;
				System.out.println("Loop:" + loop);

				for (int i = 0; i < noOfDevices; i++) {

					String id1 = String.valueOf(i);
					String values1 = deviceLatLon.get(id1);
					String[] valueArray1 = values1.split(SEPARATOR);
					double latitude1 = Double.valueOf(valueArray1[0]);
					double longitude1 = Double.valueOf(valueArray1[1]);

					moveXMeters(String.valueOf(id1));

					i++;

					String id2 = String.valueOf(i);
					String values2 = deviceLatLon.get(id2);
					String[] valueArray2 = values2.split(SEPARATOR);
					double latitude2 = Double.valueOf(valueArray2[0]);
					double longitude2 = Double.valueOf(valueArray2[1]);
					moveXMeters(String.valueOf(id2));

					Record record1 = IndexFactory.getDefaultFactory().getRecord();
					if ( loop == 0 )
						fillRecordMetadata(record1, latitude1, longitude1, id1, "1",
								randomInRange(1.0, 8.0), "enabled", city);
					else
						fillRecordLocation(record1, latitude2, longitude2, id2);

					Record record2 = IndexFactory.getDefaultFactory().getRecord();
					fillRecordLocation(record2, latitude2, longitude2, id2);
					
					store.upsert(partitioner.getPartition(record1), record1).upsert(partitioner.getPartition(record2), record2);
					records = records + 2;

					long time = 0;
					if (records % 1000 == 0) {
						long e = System.currentTimeMillis();
						time = (e - s);
						remainingTime = msTimeFor1000Records - time;
						s = System.currentTimeMillis();
					}

					if ( records % 500000 == 0 ) {
						RoadRunnerMetricFactory.getInstance().printStatus();
						System.out.println("City : " + city + " Records:" + records + " time " + time  + " micron " +
								"\t time to sleep  : " + remainingTime);
					}

				}

				Thread.sleep(timeToSleep);
			} catch (Exception e1) {
				e1.printStackTrace();
			}
    	}
    }
    
    
    public static void main(String[] args) {
    	DataWriter writer = new DataWriter(null, null, 1, "bangalore", 50);
    	String deviceId = "devices0bangalore";
    	System.out.println(writer.deviceLatLon.get(deviceId));
    	writer.moveXMeters(deviceId);
    	System.out.println(writer.deviceLatLon.get(deviceId));
    	writer.moveXMeters(deviceId);
    	System.out.println(writer.deviceLatLon.get(deviceId));
    	writer.moveXMeters(deviceId);
    	System.out.println(writer.deviceLatLon.get(deviceId));
    	writer.moveXMeters(deviceId);
    	System.out.println(writer.deviceLatLon.get(deviceId));
    	writer.moveXMeters(deviceId);
    	System.out.println(writer.deviceLatLon.get(deviceId));
    	writer.moveXMeters(deviceId);
    	System.out.println(writer.deviceLatLon.get(deviceId));
    	
	}
}
