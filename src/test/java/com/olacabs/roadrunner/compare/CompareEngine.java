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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Consumer;

import org.apache.lucene.document.DoublePoint;
import org.apache.lucene.document.FloatPoint;
import org.apache.lucene.document.IntPoint;
import org.apache.lucene.document.LongPoint;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;

import com.olacabs.roadrunner.api.GeoSpatialRecord;
import com.olacabs.roadrunner.api.GeoSpatialSearcher;
import com.olacabs.roadrunner.api.PartitionerS2;
import com.olacabs.roadrunner.api.RecordIndexes;
import com.olacabs.roadrunner.impl.PartitionBucket;

public class CompareEngine  {

    /**
     * =====================================================================================
     * ==================================SIMULATION===========================================
     * =====================================================================================
     */

    public Map<String, Device> devices = new HashMap<String, Device>();

    int range100 = 50;
    int range100000 = 100000;

    public void initializeDevices(int noOfDevices) {
        for (int i = 0; i < noOfDevices; i++) {
            Device device = new Device(i);
            devices.put(device.id, device);
        }
    }

    public static class SearchableField {
        String fieldName;
        Device.FieldType type;
        SearchOperation operation;
        Object[] val;
        Clause clause;

        public SearchableField(String fieldName, Device.FieldType type) {
            this.fieldName = fieldName; this.type = type;
            this.clause = Clause.MUST;
        }

        public SearchableField(String fieldName, Device.FieldType type, Clause clause) {
            this.fieldName = fieldName; this.type = type;
            this.clause = clause;
        }

        public SearchableField(String fieldName, Device.FieldType type, SearchOperation operation, Object[] val, Clause clause) {
            this.fieldName = fieldName;
            this.type = type;
            this.operation = operation;
            this.val = val;
            this.clause = clause;
        }
    }

    public static SearchableField randomField(Set<String> fieldNames) {
        int length = Device.FIELDS.length - 2 ; //Exclude time stamp fields
        int pos = Device.random.nextInt(length);
    	while(fieldNames.contains(Device.FIELDS[pos])) {
    		pos = Device.random.nextInt(length);
    	}
        return new SearchableField( Device.FIELDS[pos],  Device.FIELDTYPES[pos]);
    }

    public static SearchableField randomFieldNoTimeStamps() {   	
        int pos = Device.random.nextInt(Device.FIELDS.length);
        if ( pos - 2 >= 0 ) pos = pos - 2;
        return new SearchableField( Device.FIELDS[pos],  Device.FIELDTYPES[pos]);
    }

    public static int randomTotalFieldToFilter(boolean excludeTimeStamps) {
         int fldsT = Device.random.nextInt(Device.FIELDS.length) + 1;
         if ( excludeTimeStamps ) fldsT = fldsT - 2;
         return fldsT;
    }

    public static enum Clause {
        MUST, SHOULD, MUSTNOT
    }

    public static Clause randomClause() {
//        Clause[] clauses = new Clause[]{Clause.MUST, Clause.SHOULD, Clause.MUSTNOT};
        Clause[] clauses = new Clause[]{Clause.MUST, Clause.MUSTNOT};
        int pos = Device.random.nextInt(clauses.length);
        return clauses[pos];
    }

    public static enum SearchOperation {
        EQUAL, GREATERTHAN, LESSTHAN, IN, RANGE, IGNORECASEEQUAL
    }

    public static SearchOperation randomNumbericOperations() {
        SearchOperation[] numericOperations = new SearchOperation[]{
                SearchOperation.EQUAL, SearchOperation.GREATERTHAN,
                SearchOperation.LESSTHAN, SearchOperation.IN, SearchOperation.RANGE };
        int pos = Device.random.nextInt(numericOperations.length);
        return numericOperations[pos];
    }

    public static SearchOperation randomStringOperations() {
        SearchOperation[] stringOperations = new SearchOperation[]{
                //SearchOperation.EQUAL, SearchOperation.IGNORECASEEQUAL, SearchOperation.IN };
                SearchOperation.EQUAL, SearchOperation.IN };
        int pos = Device.random.nextInt(stringOperations.length);
        return stringOperations[pos];
    }

    public static enum SortOperation {
        ASC, DESC, NONE
    }

    public static SortOperation randomSortOperations() {
        SortOperation[] sortOperations = new SortOperation[]{
                SortOperation.ASC, SortOperation.DESC, SortOperation.NONE };
        int pos = Device.random.nextInt(sortOperations.length);
        return sortOperations[pos];
    }

    public static enum GroupByOperation {
        SINGLE, DOUBLE, NONE
    }

    public static GroupByOperation randomGroupByOperations() {
        GroupByOperation[] groupByOperations = new GroupByOperation[]{
                GroupByOperation.SINGLE, GroupByOperation.DOUBLE, GroupByOperation.NONE };
        int pos = Device.random.nextInt(groupByOperations.length);
        return groupByOperations[pos];
    }


    private static SearchableField[] simulateFields() {

        int totalFieldsToSearch = randomTotalFieldToFilter(true);
        if ( totalFieldsToSearch < 0 ) totalFieldsToSearch = 1;
        SearchableField[] searchFields = new SearchableField[totalFieldsToSearch];
        Set<String> fieldNames = new HashSet<>();

        for ( int i=0; i<totalFieldsToSearch; i++) {

            SearchableField fld = randomField(fieldNames);
            if ( i > 0 ) fld.clause = randomClause();
            System.out.print("+Adding Field: " + (i+1) + " of " + totalFieldsToSearch + " - " + fld.fieldName );

            fieldNames.add(fld.fieldName);
            searchFields[i] = fld;

            switch ( fld.type) {

                case BOOLEAN:
                    fld.operation = SearchOperation.EQUAL;
                    fld.val = new Object[] {( 0 == Device.random.nextInt(2) ) ? false : true};
                    break;

                case INT:
                case DOUBLE:
                case LONG:
                case FLOAT:
                    simulateFieldsNumberic(fld);
                    break;

                case STRING:
                    numbericFieldText(fld);
                    break;
            }

            System.out.println(" Operation:" + fld.operation.name() );

        }
        return searchFields;
    }

    private static void numbericFieldText(SearchableField fld) {
        SearchOperation sso = randomStringOperations();
        fld.operation = sso;
        switch ( sso ) {
            case EQUAL: {
                Double timeSubstract = Device.randomInRange(60000d, 300000d);
                fillMultiValue(fld, timeSubstract.longValue());
            }
            break;
            case IGNORECASEEQUAL: {
                Double timeSubstract = Device.randomInRange(60000d, 300000d);
                fillMultiValue(fld, timeSubstract.longValue());
            }
            break;
            case IN: {
                long x[] = new long[Device.random.nextInt(3) + 1];
                fillMultiValue(fld, x);
            }
            break;
            default:
                System.out.println("Unknown " + sso.name());
        }
    }

    private static void simulateFieldsNumberic(SearchableField fld) {
        SearchOperation nso = randomNumbericOperations();
        if(fld.fieldName.equals("FLD2") || fld.fieldName.equals("FLD7")) {
        	while(nso.name().equals(SearchOperation.IN.name()) || nso.name().equals(SearchOperation.EQUAL.name())) {
        		nso = randomNumbericOperations();
        	}
        }
        fld.operation = nso;
        switch ( nso ) {
            case EQUAL: {
                Double timeSubstract = Device.randomInRange(60000d, 300000d);
                fillMultiValue(fld, timeSubstract.longValue());
                break;
            }
            case GREATERTHAN: {
                Double timeSubstract = Device.randomInRange(300000, Long.MAX_VALUE); //Positive, Eventual Negative
                fillMultiValue(fld, timeSubstract.longValue());
                break;
            }
            case LESSTHAN: {
                Double timeSubstract = Device.randomInRange(Long.MIN_VALUE, 0); //In negative, eventual positive
                fillMultiValue(fld, timeSubstract.longValue());
                break;
            }
            case RANGE: {
                Double timeSubstract = Device.randomInRange(Long.MIN_VALUE, 0); //In negative, eventual positive
                fillMultiValue(fld, new long[] { 0L, timeSubstract.longValue() } );
                break;
            }
            case IN: {
                long x[] = new long[Device.random.nextInt(3) + 1];
                for ( int n=0; n< x.length; n++)
                    x[n] = new Double(Device.randomInRange(60000d, 300000d)).longValue();
                fillMultiValue(fld, x);
            }
            break;

        }
    }

    private static void fillMultiValue(SearchableField fld, long... timeSubstract) {
        int howMany = timeSubstract.length;

        System.out.print('>');

        switch ( fld.fieldName ) {
            case Device.FLD1:
            {
            	Set<Integer> values = new HashSet<>();
                Object[] valL = new Object[howMany];
                
                for (int i = 0; i < howMany; i++) {
                	int val = Device.randomizeFld1();
                	if(values.contains(val)) {
                		val = Device.randomizeFld1();
                	}
                	values.add(val);
                	valL[i] = val;
                }
                fld.val = valL;
            }
            break;
            case Device.FLD3:
            {
            	Set<String> values = new HashSet<>();
                Object[] valL = new Object[howMany];
                for (int i = 0; i < howMany; i++) {
                	String val = Device.randomizeFld3();
                	if(values.contains(val)) {
                		val = Device.randomizeFld3();
                	}
                	values.add(val);
                	valL[i] = val;
                }
                fld.val = valL;
            }
            break;
            case Device.FLD4:
            {
            	Set<String> values = new HashSet<>();
                Object[] valL = new Object[howMany];
                for (int i = 0; i < howMany; i++) {
                	String val = Device.randomizeFld4();
                	if(values.contains(val)) {
                		val = Device.randomizeFld4();
                	}
                	values.add(val);
                	valL[i] = val;
                }
                fld.val = valL;
            }
            break;
            case Device.FLD6:
            {
            	Set<Double> values = new HashSet<>();
                Object[] valL = new Object[howMany];
                for (int i = 0; i < howMany; i++) {
                	double val = Device.randomizeDriverScore();
                	if(values.contains(val)) {
                		val = Device.randomizeDriverScore();
                	}
                	values.add(val);
                	valL[i] = val;
                }
                fld.val = valL;
            }
            break;
            case Device.FLD5:
            {
            	Set<String> values = new HashSet<>();
                Object[] valL = new Object[howMany];
                for (int i = 0; i < howMany; i++) {
                	String val = Device.randomizeFld5();
                	if(values.contains(val)) {
                		val = Device.randomizeFld5();
                	}
                	values.add(val);
                	valL[i] = val;
                }
                
                fld.val = valL;
            }
            break;
            case Device.FLD2: {
                Object[] valL = new Object[howMany];
                for (int i = 0; i < howMany; i++) valL[i] = System.currentTimeMillis() - timeSubstract[i];
                fld.val = valL;
            }
            break;
            case Device.FLD7: {
                Object[] valL = new Object[howMany];
                for (int i = 0; i < howMany; i++) valL[i] = System.currentTimeMillis() - timeSubstract[i];
                fld.val = valL;
            }
            break;
            default:
                throw new RuntimeException("Not able to find mapping:" + fld.fieldName);
        }
        System.out.print('<');

    }


    /**
     * =====================================================================================
     * ==================================LUCENE QUERY===========================================
     * =====================================================================================
     */

    private static Query formQuery(SearchableField fld) {

        switch ( fld.type )
        {

            case BOOLEAN:
            {
                switch ( fld.operation)
                {
                    case EQUAL:
                        int val =  ((Boolean) fld.val[0] ) ? 1 : 0 ;
                        return IntPoint.newExactQuery(fld.fieldName,  val);
                    default:
                        System.err.println("fld.operation is not known.");
                        System.exit(1);
                }
            }
            break;

            case STRING:
                return formStringQuery(fld);

            case INT:
                return formIntegerQuery(fld);

            case FLOAT:
                return formFloatQuery(fld);

            case LONG:
                return formLongQuery(fld);

            case DOUBLE:
                return formDoubleQuery(fld);

            default:
                throw new RuntimeException("Not able to find mapping:" + fld.fieldName);
        }
        throw new RuntimeException("Not able to find mapping:" + fld.fieldName);

    }

    private static Query formDoubleQuery(SearchableField fld) {
        switch ( fld.operation) {
            case EQUAL:
                return DoublePoint.newExactQuery(fld.fieldName, (Double) fld.val[0]);

            case LESSTHAN:
                return DoublePoint.newRangeQuery(fld.fieldName, Long.MIN_VALUE,
                        (Double) fld.val[0] - 0.000000000000001 );

            case GREATERTHAN:
                return DoublePoint.newRangeQuery(fld.fieldName, (Double) fld.val[0] + 0.000000000000001, Long.MAX_VALUE);

            case RANGE:
                double a = (Double) fld.val[0];
                double b = (Double) fld.val[1];
                if ( a > b) {
                    double tmp = a; a = b; b = tmp;
                }
                return DoublePoint.newRangeQuery(fld.fieldName, a, b);

            case IN:
                double[] values = new double[fld.val.length];
                for (int i=0; i<values.length; i++) {
                    System.out.println("fld.val[" + i + "]" + fld.val[i]);
                    values[i] = (double) fld.val[i];
                }
                return DoublePoint.newSetQuery(fld.fieldName, values);

            default:
                System.err.println("fld.operation is not known.");
                System.exit(1);
        }
        return null;
    }

    private static Query formLongQuery(SearchableField fld) {
        switch ( fld.operation) {
            case EQUAL:
                return LongPoint.newExactQuery(fld.fieldName, (Long) fld.val[0]);

            case LESSTHAN:
                return LongPoint.newRangeQuery(fld.fieldName, Long.MIN_VALUE, (Long) fld.val[0] - 1);

            case GREATERTHAN:
                return LongPoint.newRangeQuery(fld.fieldName, (Long) fld.val[0] + 1, Long.MAX_VALUE);

            case RANGE:
                long a = (Long) fld.val[0];
                long b = (Long) fld.val[1];
                if ( a > b) {
                    long tmp = a; a = b; b = tmp;
                }
                return LongPoint.newRangeQuery(fld.fieldName, a, b);

            case IN:
                long[] values = new long[fld.val.length];
                for (int i=0; i<values.length; i++) values[i] = (long) fld.val[i];
                return LongPoint.newSetQuery(fld.fieldName, values);

            default:
                System.err.println("fld.operation is not known.");
                System.exit(1);
        }
        return null;
    }

    private static Query formFloatQuery(SearchableField fld) {

        switch ( fld.operation) {
            case EQUAL:
                return FloatPoint.newExactQuery(fld.fieldName, (Float) fld.val[0]);

            case LESSTHAN:
                return FloatPoint.newRangeQuery(fld.fieldName, Integer.MIN_VALUE, (Float) fld.val[0] - 0.0000000000001f);

            case GREATERTHAN:
                return FloatPoint.newRangeQuery(fld.fieldName, (Float) fld.val[0] + 0.000000000000001f, Integer.MAX_VALUE);

            case RANGE:
                float a = (Float) fld.val[0];
                float b = (Float) fld.val[1];
                if ( a > b) {
                    float tmp = a; a = b; b = tmp;
                }
                return FloatPoint.newRangeQuery(fld.fieldName, a, b);

            case IN:
                float[] values = new float[fld.val.length];
                for (int i=0; i<values.length; i++) values[i] = (float) fld.val[i];
                return FloatPoint.newSetQuery(fld.fieldName, values);

            default:
                System.err.println("fld.operation is not known.");
                System.exit(1);
        }
        return null;
    }

    private static Query formIntegerQuery(SearchableField fld) {
        switch ( fld.operation) {
            case EQUAL:
                return IntPoint.newExactQuery(fld.fieldName, (Integer) fld.val[0]);

            case LESSTHAN:
                return IntPoint.newRangeQuery(fld.fieldName, Integer.MIN_VALUE, (Integer) fld.val[0] - 1);

            case GREATERTHAN:
                return IntPoint.newRangeQuery(fld.fieldName, (Integer) fld.val[0] + 1, Integer.MAX_VALUE);

            case RANGE:
                System.out.println ( fld.val.length );
                int a = (Integer) fld.val[0];
                int b = (Integer) fld.val[1];
                if ( a > b) {
                    int tmp = a; a = b; b = tmp;
                }
                return IntPoint.newRangeQuery(fld.fieldName, a, b);

            case IN:
                int[] values = new int[fld.val.length];
                for (int i=0; i<values.length; i++) values[i] = (int) fld.val[i];
                return IntPoint.newSetQuery(fld.fieldName, values);

            default:
                System.err.println("fld.operation is not known.");
                System.exit(1);
        }
        return null;
    }

    private static Query formStringQuery(SearchableField fld) {
        switch ( fld.operation) {
            case EQUAL:
                return new TermQuery(new Term(fld.fieldName, fld.val[0].toString()));

            case IN:
                BooleanQuery.Builder bq = new BooleanQuery.Builder();

                for( Object objVal : fld.val) {
                    TermQuery second = new TermQuery(new Term(fld.fieldName, objVal.toString() ) );
                    bq.add(second,  BooleanClause.Occur.SHOULD);
                }
                return bq.build();

            default:
                System.err.println("fld.operation is not known.");
                System.exit(1);
        }
        return null;
    }

    /**
     * =====================================================================================
     * ==================================ROAD RUNNER QUERY===========================================
     * =====================================================================================
     */
    private static RecordIndexes roadRunnerFilterFormation(GeoSpatialSearcher localSearcher, SearchableField fld, RecordIndexes tmpRecordIndexes) {
        switch ( fld.type) {

            case BOOLEAN:

                if ( (boolean) fld.val[0])
                    tmpRecordIndexes = localSearcher.whereBoolean(fld.fieldName).isTrue();
                else
                    tmpRecordIndexes = localSearcher.whereBoolean(fld.fieldName).isFalse();

                break;

            case INT:

                switch ( fld.operation) {
                    case EQUAL:
                        tmpRecordIndexes = localSearcher.whereInt(fld.fieldName).equalTo( (int) fld.val[0]);
                        break;
                    case GREATERTHAN:
                        tmpRecordIndexes = localSearcher.whereInt(fld.fieldName).greaterThan( (int) fld.val[0]);
                        break;
                    case LESSTHAN:
                        tmpRecordIndexes = localSearcher.whereInt(fld.fieldName).lessThan( (int) fld.val[0]);
                        break;
                    case RANGE:
                        int a = (Integer) fld.val[0];
                        int b = (Integer) fld.val[1];
                        if ( a > b) {
                            int tmp = a; a = b; b = tmp;
                        }
                        tmpRecordIndexes = localSearcher.whereInt(fld.fieldName).range( a, b);
                        break;
                    case IN:
                        int[] vals = new int[fld.val.length];
                        for ( int i=0; i<vals.length; i++) {
                            vals[i] = (int) fld.val[i];
                        }
                        tmpRecordIndexes = localSearcher.whereInt(fld.fieldName).equalTo(vals);
                        break;
                }
                break;

            case FLOAT:

                switch ( fld.operation) {
                    case EQUAL:
                        tmpRecordIndexes = localSearcher.whereFloat(fld.fieldName).equalTo( (float) fld.val[0]);
                        break;
                    case GREATERTHAN:
                        tmpRecordIndexes = localSearcher.whereFloat(fld.fieldName).greaterThan( (float) fld.val[0]);
                        break;
                    case LESSTHAN:
                        tmpRecordIndexes = localSearcher.whereFloat(fld.fieldName).lessThan( (float) fld.val[0]);
                        break;
                    case RANGE:
                        float a = (Float) fld.val[0];
                        float b = (Float) fld.val[1];
                        if ( a > b) {
                            float tmp = a; a = b; b = tmp;
                        }

                        tmpRecordIndexes = localSearcher.whereFloat(fld.fieldName).range( a, b);
                        break;
                    case IN:
                        float[] vals = new float[fld.val.length];
                        for ( int i=0; i<vals.length; i++) {
                            vals[i] = (float) fld.val[i];
                        }
                        tmpRecordIndexes = localSearcher.whereFloat(fld.fieldName).equalTo(vals);
                        break;
                }
                break;


            case LONG:
                switch ( fld.operation) {
                    case EQUAL:
                        tmpRecordIndexes = localSearcher.whereLong(fld.fieldName).equalTo( (long) fld.val[0]);
                        break;
                    case GREATERTHAN:
                        tmpRecordIndexes = localSearcher.whereLong(fld.fieldName).greaterThan( (long) fld.val[0]);
                        break;
                    case LESSTHAN:
                        tmpRecordIndexes = localSearcher.whereLong(fld.fieldName).lessThan( (long) fld.val[0]);
                        break;
                    case RANGE:
                        long a = (Long) fld.val[0];
                        long b = (Long) fld.val[1];
                        if ( a > b) {
                            long tmp = a; a = b; b = tmp;
                        }

                        tmpRecordIndexes = localSearcher.whereLong(fld.fieldName).range( a, b);
                        break;
                    case IN:
                        long[] vals = new long[fld.val.length];
                        for ( int i=0; i<vals.length; i++) {
                            vals[i] = (long) fld.val[i];
                        }
                        tmpRecordIndexes = localSearcher.whereLong(fld.fieldName).equalTo(vals);
                        break;
                }
                break;

            case DOUBLE:
                switch ( fld.operation) {
                    case EQUAL:
                        tmpRecordIndexes = localSearcher.whereDouble(fld.fieldName).equalTo( (double) fld.val[0]);
                        break;
                    case GREATERTHAN:
                        tmpRecordIndexes = localSearcher.whereDouble(fld.fieldName).greaterThan( (double) fld.val[0]);
                        break;
                    case LESSTHAN:
                        tmpRecordIndexes = localSearcher.whereDouble(fld.fieldName).lessThan( (double) fld.val[0]);
                        break;
                    case RANGE:
                        double a = (Double) fld.val[0];
                        double b = (Double) fld.val[1];
                        if ( a > b) {
                            double tmp = a; a = b; b = tmp;
                        }

                        tmpRecordIndexes = localSearcher.whereDouble(fld.fieldName).range( a, b);
                        break;
                    case IN:
                        double[] vals = new double[fld.val.length];
                        for ( int i=0; i<vals.length; i++) {
                            vals[i] = (double) fld.val[i];
                        }
                        tmpRecordIndexes = localSearcher.whereDouble(fld.fieldName).equalTo(vals);
                        break;
                }
                break;

            case STRING:
                switch ( fld.operation) {
                    case EQUAL:
                        tmpRecordIndexes = localSearcher.whereString(fld.fieldName).equals( (String) fld.val[0]);
                        break;
                    case IN:
                        String[] vals = new String[fld.val.length];
                        for ( int i=0; i<vals.length; i++) {
                            vals[i] = (String) fld.val[i];
                        }
                        tmpRecordIndexes = localSearcher.whereString(fld.fieldName).in(vals);
                        break;
                }
                break;
        }
        return tmpRecordIndexes;
    }

    private static long writeToStores(boolean enableLucene, boolean enableRoadRunner, CompareEngine simulator,
                                      long s, LuceneStore luceneStore, RoadRunnerStore roadrunnerStore) throws Exception {
        PartitionBucket.COOL_OF_PERIOD = 0;
        boolean isFirstTime = true;

        for (Device device: simulator.devices.values()) {
            device.move();
        }

        int records = 0;
        long indexStartTime = System.currentTimeMillis();
        for (Device device: simulator.devices.values()) {
            if ( enableLucene ) luceneStore.add(device, isFirstTime);
            records++;
        }
        long indexEndTime = System.currentTimeMillis();
        System.out.println("L Indexing time > " + (indexEndTime - indexStartTime) + " ms");

        records = 0;
        indexStartTime = System.currentTimeMillis();
        for (Device device: simulator.devices.values()) {
            if ( enableRoadRunner ) roadrunnerStore.add(device, isFirstTime);
            records++;
        }
        indexEndTime = System.currentTimeMillis();
        System.out.println("R Indexing time > " + (indexEndTime - indexStartTime) +  " ms");

        isFirstTime = false;
        long e = System.currentTimeMillis();
        System.out.println("Total time taken : " + (e-s));
        return indexEndTime;
    }

    public static int DEVICES = 50000;
    public static int LOOPS = 100;

    public static void main(String[] args) throws Exception {
    	int noOfDevices = DEVICES;

    	CompareEngine simulator = new CompareEngine();
    	simulator.initializeDevices(noOfDevices);

    	for ( int i=0; i<LOOPS; i++)
    		if ( matchLuceneVsRoadrunner(simulator) != 0 ) break;

    }


    private static int matchLuceneVsRoadrunner(CompareEngine simulator) throws Exception {
        long startTime = System.currentTimeMillis();
        final int loops = 1;
        boolean enableLucene = true;
        boolean enableRoadRunner = true;
        final boolean isPerfTest = false;
        boolean trace = false;
        boolean isDefault = false;
        boolean manual = false;

        long s = System.currentTimeMillis();

        LuceneStore luceneStore = new LuceneStore();
        RoadRunnerStore roadrunnerStore = new RoadRunnerStore();


        long indexEndTime = writeToStores(enableLucene, enableRoadRunner, simulator, s, luceneStore, roadrunnerStore);
        Thread.sleep(10000);

        long e;
        
        int rT = 0;
        int lT = 0;
        
        for(int k = 0; k < 5; k++) {
        	/**
        	 * Start - Default Value Simulation
        	 */

        	long currentTime = System.currentTimeMillis();
        	int fld1 = Device.randomizeFld1();
        	String fld3_1 = Device.randomizeFld3();
        	String fld3_2 = Device.randomizeFld3();
        	String fld4 = Device.randomizeFld4();
        	String fld5 = Device.randomizeFld5();
        	Double fld6 = Device.randomizeFld6();

        	final double lat = Device.randomLat(fld5);
        	final double lon = Device.randomLon(fld5);
        	int radius = Device.randomizeRadius();
        	/**
        	 * END - Default Value Simulation
        	 */

        	//Random fields and operations and values simulation.

        	/*
        	 * Searchable Fields:
fld4:STRING:EQUAL:MUST:	xval1
fld3:STRING:IN:SHOULD:	yval1	yval2	yval3

Query>>+(+fld4:xval1 +fld4:xval2) (fld3:yval1 fld3:yval2 fld3:yval3)   {19.226038548573804,73.10858507150877(1012) }
        	 */
        	SearchableField[] searchFlds = null;
        	if ( manual) {
//        		searchFlds = new SearchableField[]{
//        				new SearchableField(Device.FLD6, Device.FieldType.DOUBLE, SearchOperation.LESSTHAN, new Object[] {2.0}, Clause.MUST),
//        				new SearchableField(Device.FLD3, Device.FieldType.STRING, SearchOperation.EQUAL, new Object[] {yval2}, Clause.MUSTNOT),
//        				new SearchableField(Device.FLD4, Device.FieldType.STRING, SearchOperation.IN, new Object[] {xval1}, Clause.MUSTNOT),
//        				new SearchableField(Device.FLD9, Device.FieldType.BOOLEAN, SearchOperation.EQUAL, new Object[] {false}, Clause.MUSTNOT),
//        				new SearchableField(Device.FLD1, Device.FieldType.INT, SearchOperation.GREATERTHAN, new Object[] {1}, Clause.MUST),
//        		};
        		
        		searchFlds = new SearchableField[]{
        				new SearchableField(Device.FLD4, Device.FieldType.STRING,
                                SearchOperation.EQUAL, new Object[] {"cng"}, Clause.MUST),
        				new SearchableField(Device.FLD3, Device.FieldType.STRING, SearchOperation.IN, new Object[] {
        				        Device.FLD3VALUE_5,	 Device.FLD3VALUE_1, Device.FLD3VALUE_4}, Clause.SHOULD),
        		};
        		//radius = 1000;
        	} else {
        		searchFlds = simulateFields();
        	}

        	System.out.println("Searchable Fields:");
        	for ( SearchableField fld : searchFlds) {
        		System.out.print( fld.fieldName + ":" + fld.type.name() + ":" + fld.operation.name() +  ":" + fld.clause + ":");
        		for ( Object val : fld.val) System.out.print("\t" + val.toString());
        		System.out.println("");
        	}
        	if ( searchFlds.length <= 0 ) {
        		System.out.println("No Searchable Fields. Exitting.");
        		return 0;
        	}

        	List<String> luceneIds = new ArrayList<>();
        	{
        		s = System.currentTimeMillis();
        		for ( int loop = 0; loop < loops; loop++) {

        			if ( ! enableLucene )  break;

        			if ( isDefault ) {

        				luceneIds = luceneStore.searchDefault(
        						startTime, luceneStore, indexEndTime, currentTime, fld1,
        						fld3_1, fld3_2, fld4, fld5, fld6, lat, lon, radius, trace);

        			} else {

        				int searchFldsT = searchFlds.length;

        				Query finalQuery = null;

        				if ( searchFldsT == 1 ) {

        					finalQuery = formQuery(searchFlds[0]);

        				} else {

        					finalQuery = formQuery(searchFlds[0]);

        					for ( int i=0; i<searchFldsT; i++) {
        						SearchableField fld = searchFlds[i];
        						Query partQuery = formQuery(searchFlds[i]);

        						switch ( fld.clause) {
        						case MUST:
        							BooleanQuery qryM = new BooleanQuery.Builder().add(
        									finalQuery, BooleanClause.Occur.MUST).add(
        											partQuery, BooleanClause.Occur.MUST).build();
        							finalQuery = qryM;
        							break;

        						case MUSTNOT:
        							BooleanQuery qryN = new BooleanQuery.Builder().add(
        									finalQuery, BooleanClause.Occur.MUST).add(
        											partQuery, BooleanClause.Occur.MUST_NOT).build();
        							finalQuery = qryN;
        							break;
        						case SHOULD:
        							BooleanQuery qryS = new BooleanQuery.Builder().add(
        									finalQuery, BooleanClause.Occur.SHOULD).add(
        											partQuery, BooleanClause.Occur.SHOULD).build();
        							finalQuery = qryS;
        							break;
        						}
        					}
        				}

        				System.out.println("\nQuery>>" + finalQuery.toString() +
        							"   {" + lat + "," + lon + "(" + radius + ") }\n");

        				luceneIds = luceneStore.searchIndex(finalQuery , lat, lon, radius, 1000, trace);

        			}
        		}
        		e = System.currentTimeMillis();
        		System.out.println( "L>" + (e-s) + " ms." );
        	}

        	final List<String> roadrunnerIds = new ArrayList<>(512);
        	final List<GeoSpatialRecord> roadrunnersL = new ArrayList<GeoSpatialRecord>(512);

        	{
        		s = System.currentTimeMillis();
        		for ( int loop = 0; loop < loops; loop++) {

        			if ( loop % 10000 == 0 ) System.out.println(loop);
        			if (!enableRoadRunner) break;

        			final int loopNo = loop;
        			roadrunnerIds.clear();
        			roadrunnersL.clear();

        			if ( isDefault ) {
        				roadrunnerStore.searchDefault(startTime, indexEndTime, currentTime,
        						fld1, fld3_1, fld3_2, fld4, fld5, fld6, lat, lon, radius,
        						isPerfTest, roadrunnerIds, roadrunnersL);
        			} else {
        				GeoSpatialSearcher localSearcher = roadrunnerStore.store.getDefaultGeoSpatialSearcher().
                                setRadial(lat, lon, radius).build(PartitionerS2.getS2CellsCovering(lat, lon, radius));
        				RecordIndexes finalRecordIndexes = null;
        				boolean isFirstTime = true;
        				for ( SearchableField fld: searchFlds) {

        					RecordIndexes tmpRecordIndexes = null;

        					if ( isFirstTime ) {
        						isFirstTime = false;
        						fld.clause = Clause.MUST;
        					}

        					tmpRecordIndexes = roadRunnerFilterFormation(localSearcher, fld, tmpRecordIndexes);

        					if ( null == finalRecordIndexes) {
        						finalRecordIndexes = tmpRecordIndexes;
        					} else {
        						switch ( fld.clause) {
        						case MUST:	
        							finalRecordIndexes.and(tmpRecordIndexes);
        							break;
        						case SHOULD:
        							finalRecordIndexes.or(tmpRecordIndexes);
        							break;
        						case MUSTNOT:
        							finalRecordIndexes.not(tmpRecordIndexes);
        							break;
        						}
        					}

        				}

        				if ( null != finalRecordIndexes) {
        					localSearcher.streamGeoSpatial(finalRecordIndexes).forEach(new Consumer<GeoSpatialRecord>() {
        						@Override
        						public void accept(GeoSpatialRecord geoSpatialRecord) {

        							roadrunnerIds.add(geoSpatialRecord.record.getId());
        							if  ( ! isPerfTest ) roadrunnersL.add(geoSpatialRecord);
        						}
        					});
        				}
        			}


        		}
        		e = System.currentTimeMillis();
        		System.out.println( "R>" + (e-s) + " ms");


        	}

        	rT = roadrunnerIds.size();
        	lT = luceneIds.size();

        	if(lT > rT) {
        		List<String> dupLuceneIds = new ArrayList<>();
        		dupLuceneIds.addAll(luceneIds);
        		dupLuceneIds.removeAll(roadrunnerIds);

        		for(String luceneId : dupLuceneIds) {
        			GeoSpatialRecord rec1 = new GeoSpatialRecord();
        			rec1.set(roadrunnerStore.store.get(luceneId), lat, lon, Math.cos(Math.toRadians(lat)));
        			System.err.println("Possible Error in Lucene > " +  rec1.toString() );
        		}
        	}

        	roadrunnerIds.removeAll(luceneIds);
        	System.out.println(rT +  " (Roadrunner) - " + lT + " (Lucene) = " + roadrunnerIds.size() );
        	for ( String deviceId : roadrunnerIds ) {
        		for ( GeoSpatialRecord r : roadrunnersL) {
        			if ( r.record.getId().equals(deviceId)) {
                        if((Math.abs(r.distanceInMeters() - (radius * 1.0)) / (radius * 1.0)) * 100 > 10) {
                            System.err.println("Possible Error in Lucene Distance (" +  r.distanceInMeters() + "/" +
                                    radius + ")> " +  r.toString());
                        } else {
                            System.err.println("Possible Error in Road Runner > " + r.toString());
        				}
        			}
        		}
        	}
        	long time = System.currentTimeMillis();
        	System.out.println("^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^");

        	
        	//if ((rT - lT) != 0) break;
        }
        return (rT - lT);
    }
}