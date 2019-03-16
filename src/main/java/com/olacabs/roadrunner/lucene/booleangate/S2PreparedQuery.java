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

import java.util.*;

public final class S2PreparedQuery {


    public boolean isShould = false;
    public boolean isMust = false;
    public float boost = 1.0f;

    public List<S2PreparedQuery> subQueries = new ArrayList<S2PreparedQuery>();
    public List<S2Term> terms = new ArrayList<S2Term>();


    public final void toTerms(final S2PreparedQuery query, final List<S2Term> allTerms) {
        for (S2PreparedQuery subQuery : query.subQueries) {
            toTerms(subQuery, allTerms);
        }
        allTerms.addAll(query.terms);
    }

    public final String toString(final String level) {
        StringBuilder sb = new StringBuilder();

        sb.append("\n\n >>> Query Group <<<").append(level).append(" Must [");
        sb.append(isMust).append("] : Should [").append( isShould);
        sb.append("] :Boost [").append( boost).append(']');

        if ( subQueries.size() > 0 ) sb.append("\n\n>>>Sub Queries<<<");
        for (S2PreparedQuery query : subQueries) {
            sb.append(query.toString(level + "\t"));
        }

        if ( terms.size() > 0 ) sb.append("\n\n>>>Terms<<<");
        for (S2Term term : terms) {
            sb.append(level).append(term.type).append(":").append( term.text ).append(":Must-");
            sb.append(term.isMust).append(":Should-").append( term.isShould).append(":Fuzzy-");
            sb.append(term.isFuzzy).append(":").append( term.boost);
        }
        return sb.toString();
    }

    @Override
    public final String toString() {
        return this.toString("\nQ> ");
    }


}