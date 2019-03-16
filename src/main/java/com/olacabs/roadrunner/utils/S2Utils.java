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

import java.util.ArrayList;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import com.google.common.geometry.S2Cap;
import com.google.common.geometry.S2CellId;
import com.google.common.geometry.S2CellUnion;
import com.google.common.geometry.S2LatLng;
import com.google.common.geometry.S2Region;
import com.google.common.geometry.S2RegionCoverer;
import com.olacabs.roadrunner.monitor.IRoadrunnerMetric;
import com.olacabs.roadrunner.monitor.RoadRunnerMetricFactory;


public class S2Utils {
	
	static S2Utils instance = new S2Utils();
    IRoadrunnerMetric metric = RoadRunnerMetricFactory.getInstance();

	public static S2Utils getInstance() {
		return instance;
	}
	
    public static class S2RegionWithCoverer {
        S2RegionCoverer regionCoverer;
        S2Region region;

        public S2RegionWithCoverer(S2RegionCoverer regionCoverer, S2Region region) {
            this.regionCoverer = regionCoverer;
            this.region = region;
        }
    }

    private final double EARTH_CIRCUMFERENCE_IN_METERS = 1000 * 40075.017;
    public boolean enableCache = true;
    private static Map<String, long[]> coveringCache = new ConcurrentHashMap<String, long[]>() ;
    
    private String getCacheKey(double latitude, double longitude, int radiusInMeters, int s2CellCacheKeyLevel) {
    	long s2CellId = getCellId(latitude, longitude).parent(s2CellCacheKeyLevel - 1).id();
    	return s2CellId + "_" + radiusInMeters;
    }

    private double earthMetersToRadians(double meters){
        return (2 * Math.PI) * (meters / EARTH_CIRCUMFERENCE_IN_METERS);
    }

    private S2CellId getCellId(double latitude, double longitude) {
        return S2CellId.fromLatLng(S2LatLng.fromDegrees(latitude, longitude));
    }
    
/*
 * parent - 12 is 504 cells in 3025 sqKm (55km * 55km) - average 1 cell per 6 sqKm (2.45 km * 2.45 km) 
 * parent - 13 is 1923 cells in 3025 sqKm (55km * 55km) - average 1 cell per 1.57 sqKm (1.25 km * 1.25 km) 
 * parent - 14 is 2500 cells in 3025 sqKm (55km * 55km) - average 1 cell per 1.21 sqKm (1.1 km * 1.1 km)
 * So we are choosing 13
 */
    public long getIndexCellId(double latitude, double longitude, int s2CellLevel) {
    	return getCellId(latitude, longitude).parent(s2CellLevel - 1).id();
    }

    private S2RegionWithCoverer getRegionCoverer(double latitude, double longitude, float radius, int s2CellLevel) {
        double radiusInRadians = earthMetersToRadians(radius);
        S2Region region = S2Cap.fromAxisHeight(
                S2LatLng.fromDegrees(latitude, longitude).normalized().toPoint(),
                (radiusInRadians*radiusInRadians)/2);

        S2RegionCoverer coverer = new S2RegionCoverer();
        coverer.setMinLevel(s2CellLevel);
        coverer.setMaxLevel(s2CellLevel);
        return new S2RegionWithCoverer(coverer, region);
    }

    public long[] getCellCoverForRadius(double latitude, double longitude, int radiusInMeters,  int s2CellLevel, int s2CellCacheKeyLevel) {
    	String cacheKey = null;
    	if(enableCache) {
    		cacheKey = getCacheKey(latitude, longitude, radiusInMeters, s2CellCacheKeyLevel);
    		if(coveringCache.containsKey(cacheKey)) {
                metric.increment("s2CellCacheHit", 1);
    			long[] coveringCells = coveringCache.get(cacheKey);
    			if(coveringCells != null && coveringCells.length > 0) {
    				return coveringCells;
    			}
    		}

            metric.increment("s2CellCacheMiss", 1);
    	}

        S2RegionWithCoverer rc = getRegionCoverer(latitude, longitude, radiusInMeters, s2CellLevel);
        S2CellUnion coverCells = rc.regionCoverer.getCovering(rc.region);
       
        ArrayList<S2CellId> cellIds = new ArrayList<>();
        coverCells.denormalize(s2CellLevel, 1, cellIds);
        
        long[] cellIdArray = cellIds.stream().mapToLong(cellId -> cellId.parent(s2CellLevel - 1).id()).distinct().toArray();
     
        if(enableCache) {
        	coveringCache.put(cacheKey, cellIdArray);
        }
        return cellIdArray;
    }
}