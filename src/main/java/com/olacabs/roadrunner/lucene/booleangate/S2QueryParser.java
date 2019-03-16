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

import org.apache.lucene.analysis.core.WhitespaceAnalyzer;
import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanClause.Occur;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.FuzzyQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TermRangeQuery;

public final  class S2QueryParser {

    ITinyIndexer termProcessor = null;

    private S2QueryParser() {

    }

    public S2QueryParser(ITinyIndexer tp) {
        this.termProcessor = tp;
    }

    public final S2PreparedQuery parse(final String query) throws BooleanGateExp {

        WhitespaceAnalyzer analyzer = new WhitespaceAnalyzer();

        try {

            Query qp = null;
            qp = new QueryParser("", analyzer).parse(query);
            S2PreparedQuery s2PreparedQuery = new S2PreparedQuery();
            parseComposites(qp, s2PreparedQuery);
            return s2PreparedQuery;

        } catch (ParseException ex) {
            throw new BooleanGateExp(ex);
        }

    }

    private final void parseComposites(final Query lQuery, final S2PreparedQuery s2PreparedQuery) throws BooleanGateExp {

        if(lQuery instanceof TermQuery)
        {
            populateTerm(s2PreparedQuery, false, true, lQuery);
            return;
        }

        for (BooleanClause clause : ((BooleanQuery)lQuery).clauses()) {

            Query subQueryL = clause.getQuery();

            if ( subQueryL instanceof BooleanQuery ) {

                S2PreparedQuery subQueryH = new S2PreparedQuery();
                subQueryH.isShould = clause.getOccur().compareTo(Occur.SHOULD) == 0;
                subQueryH.isMust = clause.getOccur().compareTo(Occur.MUST) == 0;

                s2PreparedQuery.subQueries.add(subQueryH);
                parseComposites(subQueryL, subQueryH);

            } else {
                boolean isShould = clause.getOccur().compareTo(Occur.SHOULD) == 0;
                boolean isMust = clause.getOccur().compareTo(Occur.MUST) == 0;
                populateTerm(s2PreparedQuery, isShould, isMust, subQueryL);
            }
        }
    }

    private final void populateTerm(final S2PreparedQuery s2PreparedQuery, final boolean isShould, final boolean isMust, final Query subQueryL)
            throws BooleanGateExp {

        S2Term s2Term = new S2Term(termProcessor);
        s2Term.isShould = isShould;
        s2Term.isMust = isMust;
        s2PreparedQuery.terms.add(s2Term);

        if ( subQueryL instanceof TermQuery ) {
            TermQuery lTerm = (TermQuery)subQueryL;
            s2Term.type = lTerm.getTerm().field();
            s2Term.text = lTerm.getTerm().text();

        } else if ( subQueryL instanceof FuzzyQuery ) {
            FuzzyQuery lTerm = (FuzzyQuery) subQueryL;
            s2Term.isFuzzy = true;
            s2Term.type = lTerm.getTerm().field();
            s2Term.text = lTerm.getTerm().text();

        } else if ( subQueryL instanceof TermRangeQuery) {
            TermRangeQuery lTerm = (TermRangeQuery) subQueryL;
            s2Term.isFuzzy = false;
            s2Term.type = lTerm.getField();
            s2Term.minRange =  lTerm.getLowerTerm().toString();
            s2Term.maxRange = lTerm.getUpperTerm().toString();

        } else {
            throw new BooleanGateExp(
                    "S2QueryParser: Not Implemented Query :" + subQueryL.getClass().toString());
        }
    }
}