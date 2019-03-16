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
package com.olacabs.roadrunner.api;

import java.util.Comparator;
import java.util.function.Function;

import com.olacabs.roadrunner.utils.Haversine;

public final class GeoSpatialRecord implements Comparable<GeoSpatialRecord>{

    public Record record;
    double distanceInMeters = Double.NaN;

    double latitude = Double.NaN;
    double longitude = Double.NaN;
    double latitudeCosine = Double.NaN;
    
    public static final String DISTANCE = "distance";
    /**
     * TODO Remove the business specific work from here.
     */
    public static final String DRY_RUN_DISTANCE = "dry_run_distance";
    
	public final void set(final Record record, final double latitude, final double longitude, final double latitudeCosine) {
        this.record = record;
        this.latitude = latitude;
        this.longitude = longitude;
        this.latitudeCosine = latitudeCosine;
        this.distanceInMeters = Double.NaN;
    }

    public double distanceInMetersCache() {
    	 if ( Double.isNaN(distanceInMeters))
    	     this.distanceInMeters = distanceInMeters();
    	 return distanceInMeters;
	}

	public final double distanceInMeters() {
        return record.measureDistanceInMeters( latitude, longitude, latitudeCosine);
    }

    public void reset() {
        record = null;
        distanceInMeters = Double.NaN;
        latitude = Double.NaN;
        longitude = Double.NaN;
        latitudeCosine = Double.NaN;
    }

    @Override
    public final int compareTo(final GeoSpatialRecord o) {
        double distanceOther = o.distanceInMetersCache();
        double distanceThis = this.distanceInMetersCache();
        
        if(Double.isNaN(distanceOther)) return -1;
        if(Double.isNaN(distanceThis)) return 1;
        if ( distanceOther > distanceThis) return -1;
        else if ( distanceOther < distanceThis) return 1;
        else return 0;
    }

    public static <T> Function<GeoSpatialRecord, T> buildFunction(final String fldName) {
        Function<GeoSpatialRecord, T> func = new Function<GeoSpatialRecord, T>() {

            @Override
            public final T apply(final GeoSpatialRecord record) {
                T val = (T) record.record.getTokenizedField(fldName);
                return val;
            }
        };
        return func;
    }
    
    public static <T extends Comparable<T>> Comparator<GeoSpatialRecord> buildComparator(final String fldName) {
    		Comparator<GeoSpatialRecord> comparator = new Comparator<GeoSpatialRecord>() {

			@Override
			public int compare(GeoSpatialRecord o1, GeoSpatialRecord o2) {
				T val1 = (T) o1.record.getTokenizedField(fldName);
				T val2 = (T) o2.record.getTokenizedField(fldName);
				
				if(val1 == null) {
					return -1;
				} else if(val2 == null) {
					return 1;
				} else {
					return val1.compareTo(val2);
				}
			}
    		
		};
        return comparator;
    }

	@Override
	public String toString() {
		return "GeoSpatialRecord [record=" + record + ", latitude=" + latitude + ", longitude=" + longitude + ", latitudeCosine=" + latitudeCosine
		        + ", distanceInMeters=" + this.distanceInMetersCache() + "]";
	}
}
