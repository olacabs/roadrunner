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
package com.olacabs.roadrunner.impl;

import java.util.ArrayList;

import com.olacabs.roadrunner.api.GeoSpatialRecord;

public class GeoSpatialRecordCache {

    private ArrayList<GeoSpatialRecord> records = new ArrayList<GeoSpatialRecord>();
    private ArrayList<GeoSpatialRecord> tracked = new ArrayList<GeoSpatialRecord>();

    public GeoSpatialRecordCache() {
    }

    public final GeoSpatialRecord take() {
        if ( records.size() > 0) {
            GeoSpatialRecord leased = records.remove(0);
            tracked.add(leased);
            return leased;
        }
        GeoSpatialRecord leased = new GeoSpatialRecord();
        tracked.add(leased);
        return leased;
    }

    public void reclaim() {
        int size = tracked.size();
        if ( size < 16) size = 16;
        for ( GeoSpatialRecord bitset : tracked) {
            bitset.reset();
        }
        records.addAll(tracked);
        tracked.clear();
        tracked.ensureCapacity(size);
    }


}
