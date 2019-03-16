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
package com.olacabs.roadrunner.impl;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;

import com.olacabs.roadrunner.api.Order;
import com.olacabs.roadrunner.api.Record;
import com.olacabs.roadrunner.api.Records;

public class RecordsImpl implements Records {

    List<Record> records = null;

    private RecordsImpl() {
        records = new ArrayList<>();
    }

    public RecordsImpl(int size) {
        records = new ArrayList<>(size);
    }

    @Override
    public List<Record> first(int first) {
        return records.subList(0, first);
    }

    @Override
    public List<Record> all() {
        return records;
    }

    public Stream<Record> stream() {
        return this.records.stream();
    }

    @Override
    public Records sort(String field, Order ascdesc) {
        return null;
    }


    @Override
    public void add(Record record) {
        records.add(record);
    }

    /* (non-Javadoc)
     * @see java.lang.Object#toString()
     */
    @Override
    public String toString() {
        return "RecordsImpl [records Size=" + records.size() + "] \n Records [" + records + "]";
    }
}
