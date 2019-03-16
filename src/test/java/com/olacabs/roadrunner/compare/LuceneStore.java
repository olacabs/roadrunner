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
/**
 *
 */
package com.olacabs.roadrunner.compare;

import com.olacabs.roadrunner.utils.Haversine;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.core.WhitespaceAnalyzer;
import org.apache.lucene.document.*;
import org.apache.lucene.index.*;
import org.apache.lucene.search.*;
import org.apache.lucene.spatial.geopoint.document.GeoPointField;
import org.apache.lucene.spatial.geopoint.search.GeoPointDistanceQuery;
import org.apache.lucene.store.RAMDirectory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class LuceneStore {

    IndexWriter writer = null;
    RAMDirectory ramDir = null;

    public LuceneStore() throws IOException{

        //Create RAMDirectory instance
        ramDir = new RAMDirectory();

        //Builds an analyzer with the default stop words
        Analyzer analyzer = new WhitespaceAnalyzer();

        // IndexWriter Configuration
        IndexWriterConfig iwc = new IndexWriterConfig(analyzer);
        iwc.setOpenMode(IndexWriterConfig.OpenMode.CREATE);

        //IndexWriter writes new index files to the directory
        this.writer = new IndexWriter(ramDir, iwc);


    }
    
    public void add(Device device, boolean isFirstTime) throws IOException{

        Document doc = new Document();
        doc.add(new StringField("ID", device.id, Field.Store.YES));
        doc.add( new GeoPointField("location", device.lon, device.lat, Field.Store.YES));
        doc.add(new StringField("lat", String.valueOf(device.lat), Field.Store.YES));
        doc.add(new StringField("lon", String.valueOf(device.lon), Field.Store.YES));

        doc.add(new LongPoint(Device.FLD2, device.last_fix_time));
        doc.add( new StoredField(Device.FLD2, device.last_fix_time ));
       
        doc.add(new LongPoint(Device.FLD7, device.status_updated_at));
        doc.add( new StoredField(Device.FLD7, device.status_updated_at ));

        if ( isFirstTime ) {

            doc.add(new IntPoint(Device.FLD1, device.status));
            doc.add( new StoredField(Device.FLD1, device.status ));

            doc.add(new StringField(Device.FLD3, device.category, Field.Store.YES));
            doc.add(new StringField(Device.FLD5, device.city, Field.Store.YES));
            doc.add(new StringField(Device.FLD4, device.fuel, Field.Store.YES));
            
            doc.add(new IntPoint(Device.FLD9, (device.flag ? 1 : 0 )));
            doc.add( new StoredField(Device.FLD9, (device.flag ? 1 : 0)));

            doc.add(new DoublePoint(Device.FLD6, device.driver_score));
            doc.add(new StoredField(Device.FLD6, device.driver_score ));
            
//            doc.add(new StringField(Device.FLD8, device.attributes, Field.Store.YES));

        }
        writer.updateDocument(new Term("ID", device.id), doc);
        writer.commit();
    }

    public List<String> searchIndex(Query query , double lat, double lng, double radius, int firstN, boolean tracer) throws IOException
    {
        IndexReader reader =  DirectoryReader.open(ramDir);
        IndexSearcher searcher = new IndexSearcher(reader);

        Query finalQuery = null;
        try {

            if ( radius > 0 ) {

                GeoPointDistanceQuery distanceQ = new GeoPointDistanceQuery("location", lng, lat, radius);
                if (null == query) {
                    finalQuery = distanceQ;
                } else {
                    BooleanQuery.Builder morphBooleanQuery = new BooleanQuery.Builder();
                    morphBooleanQuery.add(distanceQ, BooleanClause.Occur.MUST);
                    morphBooleanQuery.add(query, BooleanClause.Occur.MUST);
                    finalQuery = morphBooleanQuery.build();
                }

            } else {

                finalQuery = query;

            }
        } catch (Exception ex) {
            throw new IOException(ex);
        }

        //Query query = new TermQuery( new Term( "fuel_type", "cng") );
        //Search the index

        TopDocs foundDocs = searcher.search(finalQuery, firstN);
        int foundDoc = foundDocs.totalHits;

        //Let's print found doc names and their content along with score
        List<String> resultDocs = new ArrayList<>();
        for (ScoreDoc sd : foundDocs.scoreDocs)
        {
            Document d = searcher.doc(sd.doc);
            String id = d.get("ID");

            if ( radius > 0  ) {
            		double recordLat = Double.valueOf(d.get("lat"));
            		final double pickupLatCosine = Math.cos ( Math.toRadians(lat));
            		final double recordLatCosine = Math.cos ( Math.toRadians(recordLat));
                double distance = Haversine.distance(lat, lng,
                		recordLat, Double.valueOf(d.get("lon")), pickupLatCosine, recordLatCosine);

                if ( distance <= radius ) {
                    if ( tracer ) {
                        System.out.println(String.format("%d\t%s\t%s\t%s\t%s\t%s",
                                distance, id,
                                d.get("last_fix_time"), d.get("fuel_type"),
                                d.get("category"), d.get("status"))
                        );
                    }
                    resultDocs.add( id );
                } else {
                    /**
                    System.out.println(String.format("Lucene Skipping %f,%f - %f,%f = %f > %f",
                            lat, lng,
                            Double.valueOf(d.get("lat")), Double.valueOf(d.get("lon")),
                            distance, radius) );
                     */
                }

            } else {
                resultDocs.add(id);
            }
        }
        //don't forget to close the reader
        reader.close();

        return resultDocs;
    }

    public List<String> searchDefault(long startTime, LuceneStore luceneStore, long indexEndTime, long currentTime, int status, String category1, String category2, String fuel, String city, Double score, double lat, double lon, int radius, boolean trace) throws IOException {

        List<String> luceneIds;Query statusQuery = IntPoint.newExactQuery(Device.FLD1, status);
        Query lastFixQuery = LongPoint.newRangeQuery(Device.FLD2, startTime, currentTime);
        TermQuery categoryQuery1 = new TermQuery(new Term(Device.FLD3, category1));
        TermQuery categoryQuery2 = new TermQuery(new Term(Device.FLD3, category2));
        TermQuery fuelTypeQuery = new TermQuery(new Term(Device.FLD4, fuel));
        TermQuery cityQuery = new TermQuery(new Term(Device.FLD5, city));
        Query lastStatusUpdatedAt = LongPoint.newRangeQuery(Device.FLD7, Long.MIN_VALUE, indexEndTime);
        Query driverScoreQuery = DoublePoint.newRangeQuery(Device.FLD6, score, Long.MAX_VALUE);

        BooleanQuery categoryQuery = new BooleanQuery.Builder().add(categoryQuery1, BooleanClause.Occur.SHOULD)
                .add(categoryQuery2, BooleanClause.Occur.SHOULD).build();

        Query query1 = new BooleanQuery.Builder().add(statusQuery, BooleanClause.Occur.MUST).add(categoryQuery, BooleanClause.Occur.MUST).build();
        Query query2 = new BooleanQuery.Builder().add(query1, BooleanClause.Occur.MUST).add(fuelTypeQuery, BooleanClause.Occur.MUST).build();
        Query query3 = new BooleanQuery.Builder().add(query2, BooleanClause.Occur.MUST).add(lastFixQuery, BooleanClause.Occur.MUST).build();
        Query query4 = new BooleanQuery.Builder().add(query3, BooleanClause.Occur.MUST).add(cityQuery, BooleanClause.Occur.MUST).build();
        Query query5 = new BooleanQuery.Builder().add(query4, BooleanClause.Occur.MUST).add(lastStatusUpdatedAt, BooleanClause.Occur.MUST).build();
        Query query = new BooleanQuery.Builder().add(query5, BooleanClause.Occur.MUST).add(driverScoreQuery, BooleanClause.Occur.MUST).build();

        if  ( trace ) System.out.println("Query:" + query.toString() + " ( Radius: " + radius + " ) " + lat + "," + lon);

        luceneIds = luceneStore.searchIndex(query , lat, lon, radius, radius, trace);
        return luceneIds;
    }

    public static void main(String[] args) throws IOException {

        long startTime = System.currentTimeMillis();
        int noOfDevices = 10000;
        CompareEngine simulator = new CompareEngine();
        simulator.initializeDevices(noOfDevices);

        boolean isFirstTime = true;
        long s = System.currentTimeMillis();

        LuceneStore luceneStore = new LuceneStore();
        int i = 0;
        for (Device device: simulator.devices.values()) {
            device.move();
            luceneStore.add(device, true);
            if ( i % 1000 == 0) System.out.println("Lucene Index :" + i);
            i++;
        }
        System.out.println("Lucene Indexing completed.");
        long e = System.currentTimeMillis();
        System.out.println("Total time taken : " + (e-s));

        for ( int loop=0 ;loop<10; loop++) {
            long currentTime = System.currentTimeMillis();

            Query q1 = IntPoint.newExactQuery(Device.FLD1, 55);
            Query q2 = LongPoint.newRangeQuery(Device.FLD2, startTime, currentTime);
            TermQuery q3 = new TermQuery(new Term(Device.FLD3, Device.FLD3VALUE_2));
            TermQuery q4 = new TermQuery(new Term(Device.FLD4, "petrol"));

            Query qstg1 = new BooleanQuery.Builder()
                    .add(q1, BooleanClause.Occur.MUST).add(q3, BooleanClause.Occur.MUST).build();

            BooleanQuery qstg2 = new BooleanQuery.Builder()
                    .add(qstg1, BooleanClause.Occur.MUST).add(q4, BooleanClause.Occur.MUST).build();

            BooleanQuery qstg3 = new BooleanQuery.Builder()
                    .add(qstg2, BooleanClause.Occur.MUST).add(q2, BooleanClause.Occur.SHOULD).build();

            String s_qstg3 = qstg3.toString();
            List<String> luceneDocs = luceneStore.searchIndex(
                    qstg3, 13, 77.5, 3000, 10000, false);

            long endTime = System.currentTimeMillis();

            System.out.println( qstg3 );
            System.out.println(" > Found devices:" + luceneDocs.size() + " in " + (endTime - startTime) + " ms.");

        }


	}
}
