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

import com.google.common.geometry.S2CellId;

public final class S2Term {

    private static boolean DEBUG_MODE = true;

    public boolean isShould = false;
    public boolean isMust = false;
    public float boost = 1;
    public boolean isFuzzy = false;

    public String type = null;
    public String text = null;
    public String minRange = null;
    public String maxRange = null;

    ITinyIndexer termProcessor = null;

    private S2Term() {

    }

    public S2Term(ITinyIndexer tp) {
        this.termProcessor = tp;
    }

    public final S2QueryResult getResult(S2CellId cellId, S2PreparedQueryVariable variable) throws Exception{
        return new S2QueryResult(termProcessor.filter(cellId, this, variable));
    }

    @Override
    public final String toString() {
        return "S2Term\t" + type + ":" + text;
    }

}