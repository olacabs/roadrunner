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

import com.olacabs.roadrunner.utils.S2Utils;

public class PartitionerS2 implements Partitioner {

    public static int S2_CELL_INDEXING_LEVEL = 13;
    public static int S2_CELL_POINT_CACHEKEY_LEVEL = 17;


    @Override
    public String getPartition(Record aDocument) {
        if (!Double.isNaN(aDocument.getLat()) && aDocument.getLat() != 0.0
                && !Double.isNaN(aDocument.getLon()) && aDocument.getLon() != 0.0) {

            String newS2CellId = String.valueOf(
                    S2Utils.getInstance().getIndexCellId(aDocument.getLat(), aDocument.getLon(), S2_CELL_INDEXING_LEVEL));
            return newS2CellId;
        }
        return null;
    }

    public static String[] getS2CellsCovering(double latitude, double longitude, int radiusInMeters) {
        long[]  partitionValues = S2Utils.getInstance().getCellCoverForRadius(latitude, longitude, radiusInMeters,
                S2_CELL_INDEXING_LEVEL, S2_CELL_POINT_CACHEKEY_LEVEL);
        if ( null == partitionValues) return null;
        int partitionValuesT = partitionValues.length;
        String[] partitionValuesStr = new String[partitionValuesT];
        for(int i = 0; i < partitionValuesT; i++) {
            partitionValuesStr[i] = String.valueOf(partitionValues[i]);
        }
        return partitionValuesStr;
    }
}

