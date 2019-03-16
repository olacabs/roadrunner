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
package com.olacabs.roadrunner.compare;

import java.util.HashMap;
import java.util.Map;
import java.util.Random;


public class Device {

    public static enum FieldType {
        BOOLEAN, INT, FLOAT, LONG, DOUBLE, STRING
    }

    public static final String FLD1 = "fld1";
    public static final String FLD2 = "fld2";
    public static final String FLD3 = "fld3";
    public static final String FLD4 = "fld4";
    public static final String FLD5 = "fld5";
    public static final String FLD6 = "fld6";
    public static final String FLD7 = "fld7";
    public static final String FLD8 = "fld8";
    public static final String FLD9 = "fld9";

    /**
     * Fields and FieldTypes both need to go hand in hand, don't change one without verifying
     */
    public static final String[] FIELDS = new String[] {
            FLD1, FLD9, FLD3, FLD4,
            FLD6, FLD7, FLD2};
    public static final FieldType[] FIELDTYPES = new FieldType[] {
            FieldType.INT, FieldType.BOOLEAN,
            FieldType.STRING, FieldType.STRING, 
            FieldType.DOUBLE, FieldType.LONG, FieldType.LONG };


    public static String FLD3VALUE_1 = "jasmine";
    public static String FLD3VALUE_2 = "hibiscus";
    public static String FLD3VALUE_3 = "cannonball-tree";
    public static String FLD3VALUE_4 = "rangoon-creeper";
    public static String FLD3VALUE_5 = "champak";

    public static Random random = new Random();
    private static String[] FLD3VALUES = new String[] {FLD3VALUE_1, FLD3VALUE_2, FLD3VALUE_3, FLD3VALUE_4, FLD3VALUE_5};
    private static String[] FLD5VALUES = new String[] {"bangalore", "mumbai", "hyderabad", "chennai", "delhi"};
    private static String[] FLD4VALUES = new String[] {"petrol", "disel", "cng", "solar"};
    private static Map<String, double[]> latLonMap = new HashMap<>();
    
    static {
    	latLonMap.put("bangalore", new double[]{12.771625, 77.361946, 13.245794, 77.887917});
    	latLonMap.put("mumbai", new double[]{18.892, 72.744, 19.49636, 73.17511});
    	latLonMap.put("hyderabad", new double[]{17.16898, 78.065665, 17.610585, 78.685804});
    	latLonMap.put("chennai", new double[]{12.7104715, 79.9787093, 13.207693499, 80.320015});
    	latLonMap.put("delhi", new double[]{28.292664, 76.886324, 28.866076, 77.596999});
    }
    
    
    private static int[] statuses = new int[] {1, 2, 3, 4, 5, 6, 55, 100};

    final static double R = 6371000; // meters , earth Radius approx
    final static double PI = 3.1415926535;
    final static double r_earth = 6378;
    final static double RADIANS = PI / 180;
    final static double DEGREES = 180 / PI;
    final static double bearing = 9.71;
    final static double moveByMeters = 100;
    private final static String SEPARATOR = "_";

    public String id = "";
    public double lat = 12.67;
    public double lon = 77.5;
    public double bear = 0;
    public long last_fix_time = -1;
    public int status = 1;
    public String category = FLD3VALUE_1;
    public boolean flag = false;
    public String city = "Bangalore";
    public double driver_score = 8.0;
    public String fuel = null;
    public String attributes = null;
    public long status_updated_at = -1;

    private Device() {

    }

    public Device(int id) {
        this.id = "device" + SEPARATOR + id + SEPARATOR + "ab";
        this.last_fix_time = System.currentTimeMillis();
        this.category = randomizeFld3();
        this.fuel = randomizeFld4();
        this.attributes = randomizeAttributes();
        this.status = randomizeFld1();
        this.flag = randomizeFlag();
        this.driver_score = randomizeDriverScore();
        this.status_updated_at = System.currentTimeMillis();
        this.city = randomizeFld5();
        
        double latStart = latLonMap.get(this.city)[0];
        double latEnd = latLonMap.get(this.city)[2];
        double lonStart = latLonMap.get(this.city)[1];
        double lonEnd = latLonMap.get(this.city)[3];
        
        this.lat = randomInRange(latStart, latEnd);
        this.lon = randomInRange(lonStart, lonEnd);
        this.bear = randomInRange(0.0, 360.0);

    }

    public void move() {
        double latitude = this.lat * RADIANS;
        double longitude = this.lon * RADIANS;
        double bearing = this.bearing;

        double distance = moveByMeters / R;
        double radbear = bearing * RADIANS;

        double lat2 = Math.asin(Math.sin(latitude) * Math.cos(distance) +

                Math.cos(latitude) * Math.sin(distance) * Math.cos(radbear));

        double dlon = Math.atan2(Math.sin(radbear) * Math.sin(distance) * Math.cos(latitude),

                Math.cos(distance) - Math.sin(latitude) * Math.sin(lat2));

        double lon2 = ((longitude + dlon + Math.PI) % (Math.PI * 2)) - Math.PI;

        double newLatitude = lat2 * DEGREES;
        double newLongitude = lon2 * DEGREES;

        double latStart = latLonMap.get(this.city)[0];
        double latEnd = latLonMap.get(this.city)[2];
        double lonStart = latLonMap.get(this.city)[1];
        double lonEnd = latLonMap.get(this.city)[3];
        
        if(newLatitude < latStart || newLatitude > latEnd || newLongitude < lonStart || newLongitude > lonEnd) {
            newLatitude = randomInRange(latStart, latEnd);
            newLongitude = randomInRange(lonStart, lonEnd);
            bearing = randomInRange(0.0, 360.0);
        }
        this.lat = newLatitude;
        this.lon = newLongitude;
        this.last_fix_time = System.currentTimeMillis();
        this.status_updated_at = System.currentTimeMillis();
    }

    public static String randomizeFld3() {
        int categoryPos = random.nextInt(FLD3VALUES.length);
        return FLD3VALUES[categoryPos];
    }
    
    public static String randomizeFld5() {
        int citiesPos = random.nextInt(FLD5VALUES.length);
        return FLD5VALUES[citiesPos];
    }

    public static String randomizeFld4() {
        int fuelPos = random.nextInt(FLD4VALUES.length);
        return FLD4VALUES[fuelPos];
    }

    public static int randomizeFld1() {
        int statusPos = random.nextInt(statuses.length);
        return statuses[statusPos];
    }

    public static int randomizeRadius() {
       return random.nextInt(3500);
    }

    public static boolean randomizeFlag() {
        int flagPos = random.nextInt(2);
        if ( flagPos == 1) {
            return true;
        } else {
            return false;
        }
    }

    public static double randomizeDriverScore() {
        int rightPart = random.nextInt(1000);
        double val = ( rightPart == 0 ) ? 0 : rightPart / 100;
        return val;
    }
    
    public static double randomizeFld6() {
        int rightPart = random.nextInt(100);
        double val = ( rightPart == 0 ) ? 0 : rightPart / 100;
        return val;
    }

    public static String randomizeAttributes() {
        if ( random.nextInt(2) == 1) {
            return "attributes";
        } else {
            return null;
        }
    }

    public static double randomLat(String city) {
    	 double latStart = latLonMap.get(city)[0];
         double latEnd = latLonMap.get(city)[2];
        return randomInRange(latStart, latEnd);
    }

    public static double randomLon(String city) {
         double lonStart = latLonMap.get(city)[1];
         double lonEnd = latLonMap.get(city)[3];
        return randomInRange(lonStart, lonEnd);
    }

    public static double randomInRange(double min, double max) {
        double range = max - min;
        double scaled = random.nextDouble() * range;
        double shifted = scaled + min;
        return shifted; // == (rand.nextDouble() * (max-min)) + min;
    }

    public static void main(String[] args) {
        for ( int i=0; i<100; i++)
        System.out.println( Device.randomizeFld3() );
    }
}
