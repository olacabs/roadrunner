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

public class IndexableField<T> {

    public static enum FieldDataType {
        BOOLEAN, SHORT, INT,FLOAT,LONG,DOUBLE,STRING,
        ARRSHORT, ARRINT,ARRFLOAT,ARRLONG,ARRDOUBLE,ARRSTRING
    }

    String name;
    FieldDataType dataType;
    boolean mostlyUnique = true;
    boolean threadSafe = true;
    T defaultValue = null;

    public IndexableField(String name, boolean mostlyUnique, boolean threadSafe, FieldDataType dataType, T defaultValue) {
        this.name = name;
        this.mostlyUnique = mostlyUnique;
        this.dataType = dataType;
        this.threadSafe = threadSafe;
        this.defaultValue = defaultValue;
    }

    public String getName() {
        return this.name;
    }

    public FieldDataType getDataType() {
        return this.dataType;
    }

    public boolean isMostlyUnique() {
        return this.mostlyUnique;
    }
    
    public boolean isThreadSafe() {
        return this.threadSafe;
    }

    T getDefaultValue() {
        return this.defaultValue;
    }

	/* (non-Javadoc)
	 * @see java.lang.Object#toString()
	 */
	@Override
	public String toString() {
		return "IndexableField [name=" + name + ", dataType=" + dataType + ", mostlyUnique=" + mostlyUnique
				+ ", threadSafe=" + threadSafe + ", defaultValue=" + defaultValue + "]";
	}
    
    
}
