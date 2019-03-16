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
package com.olacabs.roadrunner.lucene.booleangate;

import java.util.BitSet;

public final class S2QueryResult {

    private BitSet foundIds = null;

    public S2QueryResult(final BitSet foundIds) {
        this.foundIds = foundIds;
    }

    public final  BitSet getRowIds() {
        if ( null == this.foundIds) {
            System.out.println(Thread.currentThread().getName() + " > S2QueryResult get has null values");
        }
        return this.foundIds ;
    }

    public final void setRowIds(final BitSet foundIds) {
        this.foundIds = foundIds;

        if ( null == this.foundIds) {
            System.out.println(Thread.currentThread().getName() + " > S2QueryResult set has null values");
        }
    }

    @Override
    public final String toString() {
        int totalIds = ( null == foundIds) ? 0 : foundIds.size();
        return "S2QueryResult=" + totalIds;
    }

}