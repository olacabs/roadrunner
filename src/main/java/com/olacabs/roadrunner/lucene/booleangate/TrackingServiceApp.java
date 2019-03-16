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

import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.StringField;

import com.olacabs.roadrunner.lucene.Radial;

import java.util.Map;

public class TrackingServiceApp {
    public static void main(String[] args) throws Exception {




        Document document = new Document();
        Field fldMiniCategory = new StringField("category", "mini", Field.Store.NO);
        Field fldSedanCategory = new StringField("category", "sedan", Field.Store.NO);



        //document.add(new StringField("categories", cat , Field.Store.YES, IndexableField.NOT_ANALYZED)); // doc is a Document



        final ITinyIndexer tp = new DummyIndexer();

        S2PreparedQuery hquery = new S2QueryParser(tp).parse("(A AND B NOT D AND P) OR ( Q AND R)");
        //System.out.println(hquery.toString());


        final int LOOPS = 200000; // 20K RPS * 9 GEO Neighbours = 200K RPS
        int CONCURRENCY =4;

        final S2PreparedQuery hqueryI = new S2QueryParser(tp).parse("(A AND B AND C AND D) OR ( E AND F)");
        final Radial radial = new Radial(12.45, 76.23, 3000);

        for ( int i=0; i<CONCURRENCY; i++) {
            new Thread(new Runnable() {
                public void run() {

                    long s = System.currentTimeMillis();
                    try {
                        for ( int i=0; i<LOOPS; i++) {

                                Map<String, S2PreparedQueryVariable> variables = S2PreparedQueryVariable.variableHolder.get();
                                variables.clear();

                                variables.put("A", new S2PreparedQueryVariable<Integer>("status", 5));
                                variables.put("B", new S2PreparedQueryVariable<String>("fuel", "petrol"));
                                variables.put("C", new S2PreparedQueryVariable<Boolean>("available", true));
                                variables.put("D", new S2PreparedQueryVariable<String>("category", "sedan"));
                                variables.put("E", new S2PreparedQueryVariable<Integer>("status", 5));
                                variables.put("F", new S2PreparedQueryVariable<Integer>("radiusmt", 1500));

                                new IndexSearcher(tp).searchDocs(radial, hqueryI, variables,
                                        new IndexSearcher.Callback() {
                                    public final void onBegin() {
                                    }

                                    public boolean receive(com.olacabs.roadrunner.lucene.Document doc) {
                                        return false;
                                    }

                                    public final boolean receive(final Document doc) {
                                        return true;
                                    }

                                    public final void onComplete() {

                                    }
                                });

                        }
                        long e = System.currentTimeMillis();
                        System.out.println( "THREAD:" + Thread.currentThread().getName() + " > LOOPS:" + LOOPS + " , Time :" + (e-s) + " ms");
                    } catch (Exception ex) {
                        ex.printStackTrace();
                        System.exit(1);
                    }

                }


            }).start();
        }




    }

}
