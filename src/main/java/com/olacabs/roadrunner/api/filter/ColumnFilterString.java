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

public interface ColumnFilterString {
	
	RecordIndexes equals(String value);
	
	RecordIndexes equalsIgnoreCase(String value);
	
	RecordIndexes in(String[] values);
	
	RecordIndexes in(String[] values, boolean ignoreCase);
	
	RecordIndexes notEquals(String value);
	
	RecordIndexes notEqualsIgnoreCase(String value);
	
	RecordIndexes notIn(String[] values);
	
	RecordIndexes notIn(String[] values, boolean ignoreCase);
	
	RecordIndexes missing();
	
	RecordIndexes exists();
	
	RecordIndexes prefix(String value);
	
	public enum Operation {
		EQUALS, EQUALS_IGNORE_CASE, IN, IN_IGNORE_CASE, NOT_EQUALS, NOT_EQUALS_IGNORE_CASE, NOT_IN, NOT_IN_IGNORE_CASE, MISSING, EXISTS, PREFIX
	}
	
}
