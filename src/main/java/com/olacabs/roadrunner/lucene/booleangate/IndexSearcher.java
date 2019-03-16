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
import java.util.Map;

import com.google.common.geometry.S2CellId;
import com.olacabs.roadrunner.lucene.Document;
import com.olacabs.roadrunner.lucene.Radial;

public final class IndexSearcher {

    private static boolean DEBUG_MODE = false;
    public static String RADIAL_FIELD = "radial";

    public static interface Callback {
        void onBegin();
        boolean receive(Document doc);
        void onComplete();
    }

    ITinyIndexer indexer = null;

    public IndexSearcher(ITinyIndexer indexer) {
        this.indexer = indexer;
    }

    // (A AND B AND C AND D) OR ( (L AND P AND P AND M) AND ( I OR K) )

    public final Map<Long, BitSet> searchIds(final Radial radial, final S2PreparedQuery query,
                                 String fieldName, Object fieldVal, String fieldOperatorGTLTINEQ,
                                             Map<Long, BitSet> previous,
                                             String mergeOperatorORNOTAND) throws BooleanGateExp {

        return null;
    }

    public final void iterator(Map<Long, BitSet> bits, String sort[], int limit, Callback callback){

    }

    public final void aggregate(Map<Long, BitSet> bits, String aggregate, int limit, Callback callback){
    }

    public final void searchDocs(final Radial radial, final S2PreparedQuery query,
                                 final Map<String, S2PreparedQueryVariable> variables,
                                 Callback callback) throws BooleanGateExp {

//        ArrayList<S2CellId> s2Cells =
//                S2Utils.getInstance().getCellIdsCoverForRadius(radial.latitude, radial.longitude, radial.radiusInMeters);
//
//        BitSetBox destination = new BitSetBox();
//
//        callback.onBegin();
//        for ( S2CellId cellId: s2Cells) {
//
//            BitSet bs = searchIds(cellId, query, variables, destination, false);
//            for (int i = bs.nextSetBit(0); i >= 0; i = bs.nextSetBit(i+1)) {
//                callback.receive(indexer.get(cellId, i));
//            }
//            destination.clear();
//        }
        callback.onComplete();
    }

    public final BitSet searchIds(
            final S2CellId cellId,
            final S2PreparedQuery query,
            final Map<String, S2PreparedQueryVariable> variables,
            final BitSetBox destination,
            boolean keepProcessingTrace) throws BooleanGateExp {

        try {
            /**
             * Imaging we are iterating through directories
             */
            for (S2PreparedQuery subQuery : query.subQueries) {
                /**
                 * Just execute the subquery.
                 */
                BitSetBox subQueryOutput = new BitSetBox();
                searchIds(cellId, subQuery, variables, subQueryOutput, keepProcessingTrace);
                if ( DEBUG_MODE ) {
                    int subQuerySize = ( null == subQueryOutput) ? 0 : subQueryOutput.size();
                    //System.out.println("Launching a Sub Query: EXIT " + subQuery.toString() + "\t" + subQuerySize);
                }

                if ( subQuery.isMust ) {

                    if ( DEBUG_MODE ) {
                        int destSize = ( null == destination) ? 0 : destination.size();
                        int subQuerySize = ( null == subQueryOutput) ? 0 : subQueryOutput.size();
                        //System.out.println("Sub Query Must: " + destination.isVirgin + "\tDestination:\t" + destSize + "\tOutput\t" + subQuerySize );
                    }
                    if ( destination.isVirgin ) {
                        destination.or(subQueryOutput);

                         if (keepProcessingTrace)
                             destination.orQueryWithFoundIdsTemp.putAll(subQueryOutput.orQueryWithFoundIds);

                    } else {
                        destination.and(subQueryOutput);
                        if (keepProcessingTrace) destination.orQueryWithFoundIdsTemp.clear();
                    }

                } else if ( subQuery.isShould ) {
                    debugAndTrace(destination, keepProcessingTrace, subQueryOutput);
                    destination.or(subQueryOutput);

                } else {
                    debugAndTrace_ANDNOT(destination, keepProcessingTrace, subQueryOutput);
                    destination.andNot(subQueryOutput);
                }

                /**
                 * In all cases, take the OR queries
                 */
                if ( keepProcessingTrace && (subQuery.isMust || subQuery.isShould ) ) {
                    destination.orQueryWithFoundIds.putAll(subQueryOutput.orQueryWithFoundIds);
                    destination.orQueryWithFoundIds.putAll(subQueryOutput.orQueryWithFoundIdsTemp);
                }

                subQueryOutput.clear();
                destination.isVirgin = false;
                debugAndTrace_SIZE(destination);
            }

            /**
             * Imaging we are iterating files of this directories
             */

            //AND Terms
            for (S2Term term : query.terms) {

                if ( ! term.isMust ) continue;

                S2QueryResult source = term.getResult(cellId, variables.get(term.text));

                if ( null == source) {
                    destination.isVirgin = false;
                    destination.clear();
                    return destination;
                }

                if ( destination.isVirgin ) {

                    //if ( DEBUG_MODE ) System.out.println("First Must :" + term.text + "\tsource:" + source);

                    destination.or(source.getRowIds());
                    destination.isVirgin = false;

                } else {
                    destination.and(source.getRowIds());

                    //if ( DEBUG_MODE ) System.out.println("Subsequent Must :" + term.text + "\n" + "source:" + source + "\tdestination\t" + destination);
                }
            }

            //OR Terms
            for (S2Term term : query.terms) {
                if ( ! term.isShould ) continue;

                S2QueryResult source = term.getResult(cellId, variables.get(term.text));
                if ( null == source) continue;

                if ( destination.isVirgin ) destination.isVirgin = false;

                //if ( DEBUG_MODE ) System.out.println( Thread.currentThread().getName() + " > OR :" + term.text);

                destination.or(source.getRowIds());
            }

            //NOT Terms
            for (S2Term term : query.terms) {

                if ( term.isShould ) continue;
                if ( term.isMust) continue;

                if ( destination.isVirgin ) {
                    throw new BooleanGateExp("Only must not query not allowed");
                } else {
                    S2QueryResult source = term.getResult(cellId, variables.get(term.text));
                    if ( null == source) continue;

                    destination.andNot( source.getRowIds());
                    //if ( DEBUG_MODE ) System.out.println("Not :" + term.text + ":");

                }
            }

            return destination;

        } catch (Exception e) {
            e.printStackTrace(System.err);
            System.out.println("Exception:" + query.toString());
            throw new BooleanGateExp(e);
        }
    }

    private final void debugAndTrace_SIZE(final BitSetBox destination) {
        if ( DEBUG_MODE ) {
            int destSize = ( null == destination) ? 0 : destination.size();
            //System.out.println("Sub Query Updated Destination: " + destSize  );
        }
    }

    private final void debugAndTrace_ANDNOT(final BitSetBox destination,
                                            final boolean keepProcessingTrace, final BitSetBox subQueryOutput) {
        if ( DEBUG_MODE ) {
            int destSize = ( null == destination) ? 0 : destination.size();
            int subQuerySize = ( null == subQueryOutput) ? 0 : subQueryOutput.size();
            //System.out.println("Sub Query Not: " + destination.isVirgin + "\tDestination:\t" + destSize + "\tOutput\t" + subQuerySize );
        }
        if (keepProcessingTrace) {
            destination.orQueryWithFoundIdsTemp.clear();
        }
    }

    private final void debugAndTrace(final BitSetBox destination,
                                     final boolean keepProcessingTrace, final BitSetBox subQueryOutput) {
        if ( DEBUG_MODE ) {
            int destSize = ( null == destination) ? 0 : destination.size();
            int subQuerySize = ( null == subQueryOutput) ? 0 : subQueryOutput.size();
            //System.out.println("Sub Query Should: " + destination.isVirgin + "\tDestination:\t" + destSize + "\tOutput\t" + subQuerySize );
        }

        if (keepProcessingTrace) {
            destination.orQueryWithFoundIds.putAll(destination.orQueryWithFoundIdsTemp);
            destination.orQueryWithFoundIds.putAll(subQueryOutput.orQueryWithFoundIds);
            destination.orQueryWithFoundIdsTemp.clear();
        }
    }

    /**
     * This is the Second term and OR is confirmed.
     */
    private final void orTrace(final BitSetBox destination, final S2Term term, final S2QueryResult source)
            throws BooleanGateExp {
        BitSetBox trace = new BitSetBox();
        trace.or(source.getRowIds());
        destination.orQueryWithFoundIds.put(term.text, trace);
        destination.orQueryWithFoundIds.putAll(destination.orQueryWithFoundIdsTemp);
    }

    /**
     * This is the Second term and AND is confirmed. Remove from temp
     */
    private final void virginAndTraceRollback(final BitSetBox destination ) {
        destination.orQueryWithFoundIdsTemp.clear();
    }

}