Build and Begin
mvn clean install


Unit tests
TODO: Should add more.

CompareTests - com.olacabs.roadrunner.compare.CompareEngine
Sample datasets are created on Lucene and Roadrunner. Same queries are sent to both and results are compared.
Currently there is some distance inaccuracies in Lucene and around 1% cases, it does not match resulting error.
These should be avoided aduring analyis phase.
TODO: Should make the city choice, density, device configurable with proper documentation

CompareTests - com.olacabs.roadrunner.compare.CompareEngine


PerormanceTests
com.olacabs.roadrunner.performance.PerfTest

Currently cities for simulation are hardcoded.
static String[] cities = new String[] {"bangalore"};

TODO:// Make it more configurable from command line.
TODO:// Performance test should add to prometheus - docker run -p 9090:9090 -v /....../roadrunner/src/etc/prometheus/prometheus.yml:/etc/prometheus/prometheus.yml prom/prometheus
TODO:// com.olacabs.roadrunner.monitor should be revisited.

GeoSpatialStore store = indexFactory.setCleanupRecordsInterval(5000).setCheckSumRecordsInterval(50000)
        .setPartitionBucketInitialCapacity(32).setPartitionBucketSubsequentIncrement(96)
        .setPartitionBucketCooloffPeriodMilliSeconds(5000).
        getStore(PerfTest.class.getSimpleName(), 2, 10000, 2, 10000);


setCleanupRecordsInterval - This is the time interval, on which the assets marked moved out are cleared.
setCheckSumRecordsInterval - This is the time where all stats are collected for reporting
setPartitionBucketInitialCapacity - How many partitions are expected. This is like initial capacity of a Vector
setPartitionBucketSubsequentIncrement - Increment size -  This is like capacityIncrement of a Vector.
setPartitionBucketCooloffPeriodMilliSeconds - This is a lockless store.
     Some ongoing search queries may see stale data if we delete the record immediately.
     The cooloff period is the timeout period of a search query. This is there to avoid concurrency issue.


TODO://Do better documentation of setting lat, lon, id and different field types.