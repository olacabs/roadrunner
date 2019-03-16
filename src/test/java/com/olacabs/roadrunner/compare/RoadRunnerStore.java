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
package com.olacabs.roadrunner.compare;

import java.util.List;
import java.util.function.Consumer;

import com.olacabs.roadrunner.api.GeoSpatialRecord;
import com.olacabs.roadrunner.api.GeoSpatialSearcher;
import com.olacabs.roadrunner.api.GeoSpatialStore;
import com.olacabs.roadrunner.api.IIndexFactory;
import com.olacabs.roadrunner.api.IndexFactory;
import com.olacabs.roadrunner.api.IndexableField;
import com.olacabs.roadrunner.api.Partitioner;
import com.olacabs.roadrunner.api.PartitionerS2;
import com.olacabs.roadrunner.api.Record;
import com.olacabs.roadrunner.api.RecordIndexes;

public class RoadRunnerStore  {

    public GeoSpatialStore store;
    public Partitioner partitioner;
    private IIndexFactory indexFactory;

    public RoadRunnerStore() {
    		indexFactory = IndexFactory.getDefaultFactory();
    		store = indexFactory.setCleanupRecordsInterval(5000).setCheckSumRecordsInterval(50000)
                .setPartitionBucketInitialCapacity(32).setPartitionBucketSubsequentIncrement(96)
                .setPartitionBucketCooloffPeriodMilliSeconds(5000).getStore(RoadRunnerStore.class.getSimpleName());

    	 	Partitioner partitioner = new PartitionerS2();
    	 	PartitionerS2.S2_CELL_INDEXING_LEVEL = 13;
    	 	PartitionerS2.S2_CELL_POINT_CACHEKEY_LEVEL = 17;
    	 	this.partitioner = partitioner;
    	 	
        indexFactory.getRecord().setIDFieldName("ID").setSpecialFieldNames("LAT", "LON");

        IndexableField[] fields = new IndexableField[9];
        fields[0] = new IndexableField<String>(Device.FLD1, false, false, IndexableField.FieldDataType.INT, null);
        fields[1] = new IndexableField<String>(Device.FLD3, false, false, IndexableField.FieldDataType.STRING, null);
        fields[2] = new IndexableField<String>(Device.FLD9, false, false, IndexableField.FieldDataType.BOOLEAN, null);
        fields[3] = new IndexableField<String>(Device.FLD5, false, false, IndexableField.FieldDataType.STRING, null);
        fields[4] = new IndexableField<String>(Device.FLD2, true, true, IndexableField.FieldDataType.LONG, null);
        fields[5] = new IndexableField<String>(Device.FLD7, true, true, IndexableField.FieldDataType.LONG, null);
        fields[6] = new IndexableField<String>(Device.FLD6, true, true, IndexableField.FieldDataType.DOUBLE, null);
        fields[7] = new IndexableField<String>(Device.FLD4, false, false, IndexableField.FieldDataType.STRING, null);
        fields[8] = new IndexableField<String>(Device.FLD8, false, false, IndexableField.FieldDataType.STRING, null);

        store.setSchema(new IndexableField<String>("ID", true, true, IndexableField.FieldDataType.STRING, null), fields);
    }

    public void add(Device device, boolean isFirstTime) throws Exception {

        Record doc = indexFactory.getRecord();
        doc.setField("ID", device.id);
        doc.setField("LAT", device.lat);
        doc.setField("LON", device.lon);
        doc.setField(Device.FLD2, device.last_fix_time);
        doc.setField(Device.FLD7, device.status_updated_at);

        if ( isFirstTime ) {

            doc.setField(Device.FLD1, device.status);
            doc.setField(Device.FLD3, device.category);
            doc.setField(Device.FLD5, device.city);
            doc.setField(Device.FLD4, device.fuel);
            doc.setField(Device.FLD9, device.flag);
            doc.setField(Device.FLD6, device.driver_score);
//            doc.setField(Device.FLD8, device.attributes);
        }

        store.upsert(partitioner.getPartition(doc), doc);
    }

    public void searchDefault(long startTime, long indexEndTime, long currentTime,
                              int status, String category1, String category2, String fuel,
                              String city, Double score, double lat, double lon, int radius,
                              boolean isPerfTest, List<String> roadrunnerIds, List<GeoSpatialRecord> roadrunnersL) {

        GeoSpatialSearcher localSearcher = store.getDefaultGeoSpatialSearcher().
                setRadial(lat, lon, radius).build(PartitionerS2.getS2CellsCovering(lat, lon, radius));
        //GeoSpatialSearcher localSearcher = store.getDefaultGeoSpatialSearcher().build();

        RecordIndexes category = localSearcher.whereString(Device.FLD3).equalsIgnoreCase(category1)
                .or(localSearcher.whereString(Device.FLD3).equalsIgnoreCase(category2));
        RecordIndexes indexes = localSearcher.whereInt(Device.FLD1).equalTo(status)
                .and(localSearcher.whereLong(Device.FLD2).range(startTime, currentTime))
                .and(category)
                .and(localSearcher.whereString(Device.FLD5).equalsIgnoreCase(city))
                .and(localSearcher.whereLong(Device.FLD7).lessThanEqualTo(indexEndTime)
                        .and(localSearcher.whereString(Device.FLD4).equalsIgnoreCase(fuel))
                        .and(localSearcher.whereDouble(Device.FLD6).greaterThanEqualTo(score)));


        localSearcher.streamGeoSpatial(indexes).forEach(new Consumer<GeoSpatialRecord>() {
            @Override
            public void accept(GeoSpatialRecord geoSpatialRecord) {

                roadrunnerIds.add(geoSpatialRecord.record.getId());
                if  ( ! isPerfTest ) roadrunnersL.add(geoSpatialRecord);
            }
        });
    }


}
