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

import java.util.Map;
import java.util.stream.Stream;

import com.olacabs.roadrunner.monitor.IPullMetric;

public interface GeoSpatialStore extends IPullMetric {


    GeoSpatialStore setSchema(IndexableField primaryKey, IndexableField[] flds);
    GeoSpatialStore setRecordValidator(IRecordValidator recordValidator);
    void setGeoFences(GeoFence[] geoFences);
    void setGeoFenceEnabled(boolean geoFenceEnabled);
   
    GeoSpatialStore addIndexableField(IndexableField field);

    /**
     * Takes the last therad local object and clears it
     * @return
     */
    GeoSpatialSearcher getDefaultGeoSpatialSearcher();

    /**
     * In a single scope of queries, this gives a new copy enabling us to do multiple radial searches.
     * @return
     */
    GeoSpatialSearcher getGeoSpatialSearcher();
    Record get(String recordId);
  
    GeoSpatialStore upsert(Record aDocument) throws Exception;
    GeoSpatialStore upsert(String partition, Record aDocument) throws Exception;
    GeoSpatialStore upsertIfFree(Record aDocument) throws Exception;
    GeoSpatialStore upsertIfFree(String partition, Record aDocument) throws Exception;
  
    GeoSpatialStore delete(String recordId) throws Exception;
    GeoSpatialStore absoluteDelete(String recordId) throws Exception;
    Map<String, Object> recordDump(String recordId);
    Stream<Record> getRecords();
}
