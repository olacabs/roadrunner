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

import java.util.ArrayList;
import java.util.LinkedList;

import com.olacabs.BitSetExposed;

public final class BitsetCache {

    private LinkedList<BitSetExposed> bits = new LinkedList<BitSetExposed>();
    private ArrayList<BitSetExposed> tracked = new ArrayList<BitSetExposed>();

    public BitsetCache() {
    }

    public final BitSetExposed take() {
        if ( bits.size() > 0) {
            BitSetExposed leased = bits.remove(0);
            tracked.add(leased);
            return leased;
        }
        BitSetExposed leased = new BitSetExposed();
        tracked.add(leased);
        return leased;
    }

    public final void reclaim() {
        for ( BitSetExposed bitset : tracked) {
            bitset.clear();
        }
        bits.addAll(tracked);
        int trackedT = tracked.size();
        tracked.clear();
        tracked.ensureCapacity(trackedT);
    }

}
