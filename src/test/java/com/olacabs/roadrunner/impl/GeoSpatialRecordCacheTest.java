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

import junit.framework.TestCase;

public class GeoSpatialRecordCacheTest extends TestCase {


    public static void main(String[] args) {
        GeoSpatialRecordCache cache = new GeoSpatialRecordCache();

        long s = System.currentTimeMillis();

        for ( int i=0; i<1000000; i++) {
            for ( int o=0; o<150; o++) {
                cache.take();
            }
            cache.reclaim();
        }

        long e = System.currentTimeMillis();
        System.out.println( 1.0 * (e-s)/1000.0 + " micron");

    }

    public void testReclaim() {

    }
}
