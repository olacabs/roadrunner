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
package com.olacabs.roadrunner.performance;

import java.util.Vector;

import java.util.concurrent.ConcurrentHashMap;

public class Driver

{

    public static void main(String[] args)

    {

        ConcurrentHashMap<String, Vector> map = new ConcurrentHashMap<>(550000);

        long start = System.currentTimeMillis();
        long mem = Runtime.getRuntime().freeMemory();
        for (int i = 0; i < 500000; i++)
        {

            Vector v = new Vector<String>(2,1);
            v.addElement(new String("Active"));
            map.put("91153710128312" + i, v);
        }
        long memE = Runtime.getRuntime().freeMemory();
        System.out.println("time taken is " + String.valueOf(System.currentTimeMillis()-start));
        System.out.println("memory " + (memE - mem));

    }

}


