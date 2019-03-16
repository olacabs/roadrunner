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

public class PartitionerHash implements  Partitioner{

    int numOfBuckets = 1;
    public PartitionerHash(int numOfBuckets) {
        this.numOfBuckets = numOfBuckets;
    }

    @Override
    public String getPartition(Record record) {
        return "bucket-" + (record.getId().hashCode() & Integer.MAX_VALUE) % numOfBuckets;
    }
}
