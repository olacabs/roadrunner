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

import com.olacabs.roadrunner.compare.CompareEngine;
import junit.framework.TestCase;

public class CompareEngineTest extends TestCase {

    public void testCompare() throws Exception {
        CompareEngine.DEVICES = 10000;
        CompareEngine.LOOPS = 5;
        CompareEngine.main(null);
        assertEquals(2, 2);
    }
}
