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
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

public class RecordHolderTest extends TestCase{

    public static void main(String[] args) {

        RecordHolder recordHolder = new RecordHolder();

        RecordImplVersion record1 = new RecordImplVersion();
        record1.setSpecialFieldNames("location.lat", "location.lon");

        Map<String, Object> location = new HashMap<>();
        location.put("lat", 12.464574);
        location.put("lon", 77.46547647);

        record1.setField(RecordImplVersion.ID, "123");
        record1.setField("location", location);

        record1.setField("status", "1");
        record1.setPos(0);
        record1.setPartitionValue("2334564345");

        recordHolder.setNewRecord(record1);

        RecordImplVersion record2 = new RecordImplVersion();
        record2.setField("status", "0");
        record2.setField("category", "abcd");

        System.out.println("Before : " + recordHolder);
        recordHolder.update(record2);
        System.out.println("After : " + recordHolder);

    }

    public void testUpdateVersion() {

    }

}
