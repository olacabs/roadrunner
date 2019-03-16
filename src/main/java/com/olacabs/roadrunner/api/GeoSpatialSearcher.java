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


import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import java.util.function.Predicate;
import java.util.stream.Stream;

import com.olacabs.BitSetExposed;
import com.olacabs.roadrunner.api.filter.ColumnFilterBoolean;
import com.olacabs.roadrunner.api.filter.ColumnFilterDouble;
import com.olacabs.roadrunner.api.filter.ColumnFilterFloat;
import com.olacabs.roadrunner.api.filter.ColumnFilterInteger;
import com.olacabs.roadrunner.api.filter.ColumnFilterLong;
import com.olacabs.roadrunner.api.filter.ColumnFilterShort;
import com.olacabs.roadrunner.api.filter.ColumnFilterString;

/**
 * <code>
 *     <b> Group Function Single</b>

 Function<GeoSpatialRecord, String> func = GeoSpatialRecord.buildFunction("category");
 Map<String, List<GeoSpatialRecord>> groupedRecords = recordStream.gauge(Collectors.groupingBy(func, Collectors.toList()));

 *
 *     <b> Group Function Double</b>

 Function<GeoSpatialRecord, String> funcCat = GeoSpatialRecord.buildFunction("category");
 Function<GeoSpatialRecord, String> funcS2 = GeoSpatialRecord.buildFunction("S2");
 Map<String, Map<String, List<GeoSpatialRecord>>> doubleGroupedRecords = recordStream.gauge(
 Collectors.groupingBy(funcS2, Collectors.groupingBy(funcCat, Collectors.toList())));

 *
 *     	<b> Group Function Double Concat</b>

 Function<GeoSpatialRecord, String> funcCat = GeoSpatialRecord.buildFunction("category");
 Function<GeoSpatialRecord, String> funcS2 = GeoSpatialRecord.buildFunction("S2");
 Function<GeoSpatialRecord, String> funcId = GeoSpatialRecord.buildFunction("ID");
 Map<String, Map<String, String>> doubleGroupedRecords = recordStream.gauge(
 Collectors.groupingBy(funcS2, Collectors.groupingBy(funcCat,
 Collectors.mapping(funcId, Collectors.joining(",")))));

 *     <b> Group Function Single Sort</b>
 Stream<GeoSpatialRecord> recordStreamLoop = localSearcher.streamGeoSpatial(indexes, latitude, longitude);
 Function<GeoSpatialRecord, String> funcGeoSpatial = GeoSpatialRecord.buildFunction("category");
 Map<String, List<GeoSpatialRecord>> groupedRecordsP =
    recordStreamLoop.gauge(Collectors.groupingBy(funcGeoSpatial, Collectors.toList()));
 for ( List<GeoSpatialRecord> gpL : groupedRecordsP.values()) {
    gpL.stream().sorted();
 }


 * </code>
 */
public interface GeoSpatialSearcher {

	GeoSpatialSearcher setRadial(double latitude, double longitude, int radiusInMeters);
	
	GeoSpatialSearcher build();
	
	GeoSpatialSearcher build(String[] partitionValues);
	
	GeoSpatialSearcher buildByIds(String[] ids);
	
	ColumnFilterBoolean whereBoolean(String columnName);

	ColumnFilterShort whereShort(String columnName);

	ColumnFilterInteger whereInt(String columnName);

	ColumnFilterFloat whereFloat(String columnName);

	ColumnFilterDouble whereDouble(String columnName);

	ColumnFilterLong whereLong(String columnName);

	ColumnFilterString whereString(String columnName);

	Records records(RecordIndexes indexes);

	void forEach(RecordIndexes indexes, Consumer<Record> action);

	Stream<Record> stream(RecordIndexes indexes);
	
	<T extends Object> Map<T, List<Record>> stream(RecordIndexes indexes,List<Predicate<Record>> predicates, String groupingBy,
			   Map<T, List<Record>> groupedRecords) throws IllegalArgumentException;

	Stream<Record> stream(RecordIndexes indexes, List<Predicate<Record>> predicates, String sortingBy, int limit);

	Stream<GeoSpatialRecord> streamGeoSpatial(RecordIndexes indexes);

	<T extends Object> Map<T, List<GeoSpatialRecord>> streamGeoSpatial(
			RecordIndexes indexes, List<Predicate<GeoSpatialRecord>> predicates, 
			String groupingBy, String sortingBy, int limit,
			Map<T, List<GeoSpatialRecord>> groupedRecords) throws IllegalArgumentException;

	Stream<GeoSpatialRecord> streamGeoSpatial(RecordIndexes indexes, List<Predicate<GeoSpatialRecord>> predicates, String sortingBy, int limit);

	<T extends Object> Map<T, List<GeoSpatialRecord>> streamGeoSpatial(RecordIndexes indexes, List<Predicate<GeoSpatialRecord>> predicates,
																					String groupingBy, Map<T, List<GeoSpatialRecord>> groupedRecords) throws IllegalArgumentException;

	BitSetExposed[] getResultDocIds();

	void setResultDocIds(BitSetExposed[] resultBitset);

}