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
package com.olacabs.roadrunner.utils;

import java.util.Random;

public final class Haversine {

    public static final class Kappa {

        static final float PI = 3.1415927f;
        static final float PI_2 = PI / 2f;
        static final float MINUS_PI_2 = -PI_2;

        public static final double atan2(final double y, final double x) {
            if (x == 0.0f) {
                if (y > 0.0f) {
                    return PI_2;
                }
                if (y == 0.0f) {
                    return 0.0f;
                }
                return MINUS_PI_2;
            }

            final double atan;
            final double z = y / x;
            if (Math.abs(z) < 1.0f) {
                atan = z / (1.0f + 0.28f * z * z);
                if (x < 0.0f) {
                    return (y < 0.0f) ? atan - PI : atan + PI;
                }
                return atan;
            } else {
                atan = PI_2 - z / (z * z + 0.28f);
                return (y < 0.0f) ? atan - PI : atan;
            }
        }
    }

    private static final double EARTH_RADIUS = 6378137; // Approx Earth radius in KM
    private static final double EARTH_RADIUS2 = EARTH_RADIUS * 2; // Approx Earth radius in KM
    public static final double PIBY180 = Math.PI / 180.0  ; // Approx Earth radius in KM

    public final static double cosapprox(double val) {
        double val2 = val*val;
        return 0.999959766864776611328125f + val2 * (-0.4997930824756622314453125f + val2 * (4.1496001183986663818359375e-2f +
                val2 * (-1.33926304988563060760498046875e-3f + val2 * 1.8791708498611114919185638427734375e-5f)));
    }

    public final static double distanceFast(final double startLat, final double startLong,
                                        final double endLat, final double endLong) {

        double startLatRad = startLat * PIBY180;
        double endLatRad = endLat * PIBY180;
        return Haversine.distance(startLat, startLong, endLat, endLong,
                cosapprox ( startLatRad ), cosapprox ( endLatRad ));
    }


    public final static double distance(final double startLat, final double startLong,
                                        final double endLat, final double endLong,
                                        final double cosStartLat, final double cosEndLat) {

        double dLat = ((endLat - startLat)) * PIBY180 ;
        double dLong =((endLong - startLong))  * PIBY180 ;

        double sinLatInput = dLat / 2;
        double sinLatInputSqre = sinLatInput * sinLatInput;
        double hdLatSin = sinLatInput * (0.99997937679290771484375f +
                sinLatInputSqre * (-0.166624367237091064453125f +
                        sinLatInputSqre * (8.30897875130176544189453125e-3f +
                                sinLatInputSqre * (-1.92649182281456887722015380859375e-4f +
                                        sinLatInputSqre * 2.147840177713078446686267852783203125e-6f))));

        double hdLat = hdLatSin * hdLatSin;

        double sinLonInput = dLong / 2;
        double sinLonInputSqre = sinLonInput * sinLonInput;
        double hdLonSin = sinLonInput * (0.99997937679290771484375f +
                sinLonInputSqre * (-0.166624367237091064453125f +
                        sinLonInputSqre * (8.30897875130176544189453125e-3f +
                                sinLonInputSqre * (-1.92649182281456887722015380859375e-4f +
                                        sinLonInputSqre * 2.147840177713078446686267852783203125e-6f))));


        double hdLong = hdLonSin * hdLonSin;

        double a = hdLat + cosStartLat * cosEndLat * hdLong;
        double sqrtA = Math.sqrt(a);
        double sqrtB = Math.sqrt(1 - a);

        double tanVal = Kappa.atan2(sqrtA, sqrtB);
        //double tanVal = Math.atan2(sqrtA, sqrtB);
      	return EARTH_RADIUS2 * tanVal;
    }

    /**
     * Calculate the haversine distance betweeen 2 points.
     *
     * @param startLat Latitude (in degress) of the build point
     * @param startLong Longitude (in degress) of the build point
     * @param endLat Latitude (in degress) of the end point
     * @param endLong Longitude (in degress) of the end point
     * @return
     */
    public final static double distance(double startLat, final double startLong,
                                        double endLat, final double endLong) {

        double dLat = Math.toRadians((endLat - startLat));
        double dLong = Math.toRadians((endLong - startLong));
        startLat = Math.toRadians(startLat);
        endLat = Math.toRadians(endLat);
        double a = haversin(dLat) + Math.cos(startLat) * Math.cos(endLat)
                * haversin(dLong);
        double c = 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a));
        return EARTH_RADIUS * c;


    }
    public static final double haversin(final double val) {
        return Math.pow(Math.sin(val / 2), 2);
    }

    public static void main(String[] args) {
        double startLat = 12.840384915913592;
        double startLong = 77.58116734186618;
        double endLat = 12.917718031756717;
        double endLong = 77.65331815210727;

        Random rand = new Random();
        for ( int i=0; i<100; i++) {

            startLat = 1000000 + rand.nextInt(500000);
            startLat = 1.0 * startLat / 100000.0;

            endLat = 1000000 + rand.nextInt(500000);
            endLat = 1.0 * endLat / 100000.0;

            startLong = 7500000 + rand.nextInt(500000);
            startLong = 1.0 * startLat / 100000.0;

            endLong = 7500000 + rand.nextInt(500000);
            endLong = 1.0 * endLat / 100000.0;

            int actual = new Double(Haversine.distance(startLat, startLong, endLat, endLong)).intValue();
            int fast = new Double(Haversine.distanceFast(startLat, startLong, endLat, endLong)).intValue();
            int variation = new Double( Math.abs((actual - fast)) ).intValue();
            double variationPct = (1.0 * variation/actual) * 100;
            if ( variationPct > .005) {
                System.out.println ("*****\n   Actual:" +  actual +  " , Deviation = " + variation + " , Pct:" +  variationPct + "\n*****") ;
            } else {
                System.out.println ("=   Actual:" +  actual +  " , Deviation = " + variation + " , Pct:" +  variationPct) ;
            }
        }

        double cosStartLat = Math.cos ( Math.toRadians(startLat) );
        double cosEndLat = Math.cos ( Math.toRadians(endLat) );


        for ( int loop=0; loop<1; loop++) {
			int total = 100000000;

            long s = System.currentTimeMillis();
            for ( int i=0; i<total; i++) {
                Haversine.distance(startLat, startLong, endLat, endLong);
            }
            System.out.println("Regular Haversine>" + ( System.currentTimeMillis() -s ));

            s = System.currentTimeMillis();
            for ( int i=0; i<total; i++) {
                Haversine.distanceFast(startLat, startLong, endLat, endLong);
            }
            System.out.println("Fast Haversine>" + ( System.currentTimeMillis() - s));

            s = System.currentTimeMillis();
            for ( int i=0; i<total; i++) {
                Haversine.distance(startLat, startLong, endLat, endLong, cosStartLat, cosEndLat);
            }
            System.out.println("Cached Fast Haversine>" + ( System.currentTimeMillis() - s));

		}

	}
}