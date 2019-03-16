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

public class S2UtilsTest {

    public static void main(String[] args) {
        double latitude = 12.972462;
        double longitude = 77.593518;
        int s2CellLevel = 13;
        int s2CellCacheKeyLevel = 17;
        System.out.println(S2Utils.instance.getIndexCellId(latitude, longitude, s2CellCacheKeyLevel));
        System.out.println(S2Utils.instance.getIndexCellId(latitude + .0001, longitude + + .0001, s2CellCacheKeyLevel));
        System.out.println(S2Utils.instance.getIndexCellId(latitude + .001, longitude + + .001,s2CellCacheKeyLevel));
        System.out.println(S2Utils.instance.getIndexCellId(latitude + .005, longitude + + .005, s2CellCacheKeyLevel));
        System.out.println(S2Utils.instance.getIndexCellId(latitude + .01, longitude + + .01, s2CellCacheKeyLevel));
        System.out.println(S2Utils.instance.getIndexCellId(latitude + .1, longitude + + .1, s2CellCacheKeyLevel));
        System.out.println(S2Utils.instance.getIndexCellId(latitude + 1, longitude + 1, s2CellCacheKeyLevel));

        long[] cellIds = S2Utils.instance.getCellCoverForRadius(latitude, longitude, 3000, s2CellLevel , 17);

        System.out.println("Cell ids length : " + cellIds.length);
        for(int i = 0; i < cellIds.length; i++) {
            System.out.println("Cell id : " + cellIds[i]);
        }
        S2Utils.getInstance().enableCache = false;
        for(int j=0; j < 1; j++) {
            long s = System.currentTimeMillis();
            for(int i= 0; i < 1; i++) {
                latitude = latitude + .00005;
                longitude+= 0.00005;
                S2Utils.getInstance().getCellCoverForRadius(latitude, longitude, 5000, s2CellLevel, s2CellCacheKeyLevel);
            }
            long e = System.currentTimeMillis();
            System.out.println(e-s);
        }

    }
}
