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

package com.olacabs.roadrunner.api.filter;

import com.olacabs.roadrunner.api.RecordIndexes;

/**
 * Don't Edit this code, it is generated using templates
 */
public interface ColumnFilterFloat {
	
    RecordIndexes greaterThan(float value);
    RecordIndexes lessThan(float value);
    RecordIndexes greaterThanEqualTo(float value);
    RecordIndexes lessThanEqualTo(float value);
    RecordIndexes equalTo(float value);
    RecordIndexes equalTo(float[] values);
    RecordIndexes range(float minValue, float maxValue);
    RecordIndexes missing();
	RecordIndexes exists();
    
    public static enum Operation {
		GREATER_THAN, GREATER_THAN_EQUAL_TO, LESSER_THAN, LESSER_THAN_EQUAL_TO, EQUAL_TO, RANGE, MISSING, EXISTS
	}
}

