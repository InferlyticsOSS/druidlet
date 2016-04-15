# druidlet - Embedded Druid for testing

[`Druid`](http://druid.io/druid.html "Druid") is an open-source analytics data store designed for business intelligence (OLAP) queries on event data. Druid provides low latency (real-time) data ingestion, flexible data exploration, and fast data aggregation. Existing Druid deployments have scaled to trillions of events and petabytes of data. Druid is most commonly used to power user-facing analytic applications.

**druidlet** is a sub-set of `Druid`, allowing simple index creation and querying from an embedded instance. It's based on v0.9.0 of Druid.

##Why druidlet?

**druidlet** is very useful when:

1. You have to test some code that depends on Druid. Setting up Druid on your machine may not be practical as it requires a lot of other components to work.
2. You might have a build environment that runs a few tests before packaging your project, and it might not make sense to run Druid on that machine.
3. You might want to leverage some of the cool functionality that Druid provides, on a much smaller scale. 

##Build Status

**druidlet** is configured on Travis CI. The current status of the master branch is given below:

![](https://travis-ci.org/InferlyticsOSS/druidlet.svg?branch=master)

##Usage

###Requirements

1. Java (1.7+ maybe, as that's what this was written in. If you can get it working with older versions, please drop a note)
2. Maven

###Including in your project

####As a Maven dependency

**druidlet** is on Bintray and Maven Central:

    <dependency>
        <groupId>com.inferlytics</groupId>
        <artifactId>druidlet</artifactId>
        <version>0.1.1</version>
    </dependency>

####As a JAR

Clone this repository and build the JAR using:

	mvn clean package

This should generate the `druidlet-0.1.0.jar` in your `./target` folder.

###Indexing and Querying

####Indexing from CSV

`QueryableIndex` objects can be queried using the `QueryExecutor.run()` method. The `QueryableIndex` can be built as follows:

    Reader reader = new FileReader(new File("/path/to/file/file.csv"));

    List<String> columns = Arrays.asList("dim1", "dim2", "ts", "metric", "value", "count", "min", "max", "sum");
    List<String> metrics = Arrays.asList("value", "count", "min", "max", "sum");
    List<String> dimensions = new ArrayList<>(columns);
    dimensions.removeAll(metrics);
    Loader loader = Loader.csv(reader, columns, dimensions, "ts");

    DimensionsSpec dimensionsSpec = new DimensionsSpec(dimensions, null, null);
    AggregatorFactory[] metricsAgg = new AggregatorFactory[]{
            new LongSumAggregatorFactory("agg_count", "count"),
            new DoubleMaxAggregatorFactory("agg_max", "max"),
            new DoubleMinAggregatorFactory("agg_min", "min"),
            new DoubleSumAggregatorFactory("agg_sum", "sum")
    };
    IncrementalIndexSchema indexSchema = new IncrementalIndexSchema(0, QueryGranularity.ALL, dimensionsSpec, metricsAgg);
    DruidIndices.getInstance().cache(indexKey, loader, indexSchema);

The call to `DruidIndices.getInstance().cache(...)` builds the index and caches it with the key specified by the `indexKey`, which can be any String.

####Querying through Code

Indexes can be obtained using `DruidIndices.getInstance().get(indexKey)`. They can be queried as follows:
    
    List<DimFilter> filters = new ArrayList<DimFilter>();
    filters.add(DimFilters.dimEquals("report", "URLTransaction"));
    filters.add(DimFilters.dimEquals("pool", "r1cart"));
    filters.add(DimFilters.dimEquals("metric", "Duration"));
    Query query = GroupByQuery.builder()
        .setDataSource("test")
        .setQuerySegmentSpec(QuerySegmentSpecs.create(new Interval(0, new DateTime().getMillis())))
        .setGranularity(QueryGranularity.NONE)
        .addDimension("dim1")
        .addAggregator(new LongSumAggregatorFactory("agg_count", "agg_count"))
        .addAggregator(new DoubleMaxAggregatorFactory("agg_max", "agg_max"))
        .addAggregator(new DoubleMinAggregatorFactory("agg_min", "agg_min"))
        .addAggregator(new DoubleSumAggregatorFactory("agg_sum", "agg_sum"))
        .setDimFilter(DimFilters.and(filters))
        .build();

    Sequence<Row> sequence = QueryExecutor.run(query, index);

The result is contained in the `Sequence`.

####Querying via HTTP

First off, you need to start **druidlet** from the `DruidRunner` class:

    new DruidRunner(37843, index).run();

Here the first parameter is the `PORT` you want **druidlet** to listen on. The second parameter is the `QueryableIndex` you want to be able to query, created as mentioned in the `Indexing from CSV` section.

Once **druidlet** is running, you can query it via REST calls:

	curl -X POST --header 'Content-Type: application/json' --header 'Accept: application/json' -d '{...}' 'http://localhost:37843/druid/v2'

Once [jDruid](https://github.com/InferlyticsOSS/jDruid) is ready, it can be used to query **druidlet** as well.

##What's next?

**druidlet** is missing some of the following:

1. Indexing from other sources
2. Support for Windows (Currently there are some Memory Mapped Files which cause issues)
3. Stand-alone execution from the command line
4. Maven Central and JCenter
5. Any other missing features that people point out
6. Lightweight HTTP server (Jetty is lightweight, but we can go lighter!)


Whether these features will be made available soon or never depends on how useful the current set of features are 

##Help

If you face any issues trying to get druidlet to work for you, please send an email to sriram@raremile.com

##References

This project was made possible thanks to:

1. eBay's [embedded-druid](https://github.com/eBay/embedded-druid) project which provided some of the early code.
2. `pjain11` on `#druid-dev` on `irc.freenode.net` who helped with some serialization/deserialization issues.
