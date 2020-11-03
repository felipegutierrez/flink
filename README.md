# Apache Flink

This is an [Apache Flink](https://github.com/apache/flink) branch forked from the original repository of Apache Flink version 1.11.2.

## Compiling

```
$ git clone https://github.com/felipegutierrez/flink.git flink-adcom
$ cd flink-adcom

// Compiling the whole project
$ mvn clean install -e -X -DskipTests -Dskip.npm -Dmaven.javadoc.skip=true -Dcheckstyle.skip=true -Drat.skip=true -Dscala-2.12
$ mvn clean install -DskipTests -Dfast -Dscala-2.12
$ ll flink-examples/flink-examples-streaming/target/

// Compiling only the adcom project. The Flink project has to be compiled before
$ mvn clean install -DskipTests -Dfast -Dscala-2.12 -rf :flink-adcom_2.12
```

## Running

Set the frequency of the data generator:
```
echo "1000000" > /tmp/datarate.txt # 1K rec/sec
echo "20000" > /tmp/datarate.txt   # 50K rec/sec
echo "10000" > /tmp/datarate.txt   # 100K rec/sec
echo "2000" > /tmp/datarate.txt    # 500K rec/sec
echo "1000" > /tmp/datarate.txt    # 1M rec/sec
```
### Taxi ride event count query
This query is available at this [TaxiRideCountPreAggregate.java](flink-examples/flink-examples-streaming/src/main/java/org/apache/flink/streaming/examples/aggregate/TaxiRideCountPreAggregate.java).
```
StreamExecutionEnvironment env = ...;
env.addSource(new TaxiRideSourceUDF())
   .map(new TaxiRideTupleUDF())
   .combiner(new TaxiRideSumUDF())
   .keyBy(new TaxiRideKeySelectorUDF())
   .reduce(new TaxiRideSumUDF())
   .sink(new PrintUDF());
```
Running the **static combiner** with 8 combiners and 8-16-24 reducers, or with parallelism 16 for all:
```
./bin/flink run ../flink-applications/TaxiRideCountPreAggregate.jar -pre-aggregate-window-timeout 1 -controller false -slotSplit 2 -disableOperatorChaining true -input /home/flink/flink-applications/nycTaxiRides.gz -output mqtt -sinkHost IP_r02
./bin/flink run ../flink-applications/TaxiRideCountPreAggregate.jar -pre-aggregate-window-timeout 1 -controller false -slotSplit 1 -parallelism-group-02 16 -disableOperatorChaining true -input /home/flink/flink-applications/nycTaxiRides.gz -output mqtt -sinkHost IP_r02
./bin/flink run ../flink-applications/TaxiRideCountPreAggregate.jar -pre-aggregate-window-timeout 1 -controller false -slotSplit 1 -parallelism-group-02 24 -disableOperatorChaining true -input /home/flink/flink-applications/nycTaxiRides.gz -output mqtt -sinkHost IP_r02
./bin/flink run -p 16 ../flink-applications/TaxiRideCountPreAggregate.jar -pre-aggregate-window-timeout 1 -controller false -slotSplit 2 -disableOperatorChaining true -input /home/flink/flink-applications/nycTaxiRides.gz -output mqtt -sinkHost IP_r04
```
Running the **autonomous combiner** with 8 combiners and 8-16-24 reducers, or with parallelism 16 for all:
```
./bin/flink run ../flink-applications/TaxiRideCountPreAggregate.jar -controller true -slotSplit 2 -disableOperatorChaining true -input /home/flink/flink-applications/nycTaxiRides.gz -output mqtt -sinkHost IP_r02
./bin/flink run ../flink-applications/TaxiRideCountPreAggregate.jar -controller true -slotSplit 1 -parallelism-group-02 16 -disableOperatorChaining true -input /home/flink/flink-applications/nycTaxiRides.gz -output mqtt -sinkHost IP_r02
./bin/flink run ../flink-applications/TaxiRideCountPreAggregate.jar -controller true -slotSplit 1 -parallelism-group-02 24 -disableOperatorChaining true -input /home/flink/flink-applications/nycTaxiRides.gz -output mqtt -sinkHost IP_r02
./bin/flink run -p 16 ../flink-applications/TaxiRideCountPreAggregate.jar -controller true -slotSplit 2 -disableOperatorChaining true -input /home/flink/flink-applications/nycTaxiRides.gz -output mqtt -sinkHost IP_r04
```
### Taxi ride top-N query
This query is available at this [TaxiRideDistanceTopNPreAggregate.java](flink-examples/flink-examples-streaming/src/main/java/org/apache/flink/streaming/examples/aggregate/TaxiRideDistanceTopNPreAggregate.java).
```
./bin/flink run ../flink-applications/TaxiRideDistanceTopNPreAggregate.jar -topN 100 -controller true -slotSplit 2 -disableOperatorChaining true -input /home/flink/flink-applications/nycTaxiRides.gz -output mqtt -sinkHost IP_r02
./bin/flink run ../flink-applications/TaxiRideDistanceTopNPreAggregate.jar -topN 100 -controller true -slotSplit 1 -parallelism-group-02 16 -disableOperatorChaining true -input /home/flink/flink-applications/nycTaxiRides.gz -output mqtt -sinkHost IP_r02
./bin/flink run ../flink-applications/TaxiRideDistanceTopNPreAggregate.jar -topN 100 -controller true -slotSplit 1 -parallelism-group-02 24 -disableOperatorChaining true -input /home/flink/flink-applications/nycTaxiRides.gz -output mqtt -sinkHost IP_r02
./bin/flink run ../flink-applications/TaxiRideDistanceTopNPreAggregate.jar -topN 100 -pre-aggregate-window-timeout 1 -controller false -slotSplit 2 -disableOperatorChaining true -input /home/flink/flink-applications/nycTaxiRides.gz -output mqtt -sinkHost IP_r02
./bin/flink run ../flink-applications/TaxiRideDistanceTopNPreAggregate.jar -topN 100 -pre-aggregate-window-timeout 1 -controller false -slotSplit 1 -parallelism-group-02 16 -disableOperatorChaining true -input /home/flink/flink-applications/nycTaxiRides.gz -output mqtt -sinkHost IP_r02
./bin/flink run ../flink-applications/TaxiRideDistanceTopNPreAggregate.jar -topN 100 -pre-aggregate-window-timeout 1 -controller false -slotSplit 1 -parallelism-group-02 24 -disableOperatorChaining true -input /home/flink/flink-applications/nycTaxiRides.gz -output mqtt -sinkHost IP_r02
```
```
StreamExecutionEnvironment env = ...;
env.addSource(new TaxiRideSourceUDF())
   .map(new TaxiRideTupleUDF())
   .combiner(new TopNPassengerPreAggUDF(100))
   .keyBy(new TaxiRideKeySelectorUDF())
   .reduce(new TopNPassengerReducerUDF(100))
   .sink(new PrintUDF());
```
### Taxi ride average query
This query is available at this [TaxiRideAveragePreAggregate.java](flink-examples/flink-examples-streaming/src/main/java/org/apache/flink/streaming/examples/aggregate/TaxiRideAveragePreAggregate.java).
```
./bin/flink run ../flink-applications/TaxiRideAveragePreAggregate.jar -pre-aggregate-window-timeout 1 -controller false -slotSplit 2 -disableOperatorChaining true -input /home/flink/flink-applications/nycTaxiRides.gz -output mqtt -sinkHost IP
./bin/flink run ../flink-applications/TaxiRideAveragePreAggregate.jar -pre-aggregate-window-timeout 1 -controller false -slotSplit 1 -parallelism-group-02 16 -disableOperatorChaining true -input /home/flink/flink-applications/nycTaxiRides.gz -output mqtt -sinkHost IP
./bin/flink run ../flink-applications/TaxiRideAveragePreAggregate.jar -pre-aggregate-window-timeout 1 -controller false -slotSplit 1 -parallelism-group-02 24 -disableOperatorChaining true -input /home/flink/flink-applications/nycTaxiRides.gz -output mqtt -sinkHost IP
./bin/flink run ../flink-applications/TaxiRideAveragePreAggregate.jar -controller true -slotSplit 2 -disableOperatorChaining true -input /home/flink/flink-applications/nycTaxiRides.gz -output mqtt -sinkHost IP
./bin/flink run ../flink-applications/TaxiRideAveragePreAggregate.jar -controller true -slotSplit 1 -parallelism-group-02 16 -disableOperatorChaining true -input /home/flink/flink-applications/nycTaxiRides.gz -output mqtt -sinkHost IP
./bin/flink run ../flink-applications/TaxiRideAveragePreAggregate.jar -controller true -slotSplit 1 -parallelism-group-02 24 -disableOperatorChaining true -input /home/flink/flink-applications/nycTaxiRides.gz -output mqtt -sinkHost IP
```
```
StreamExecutionEnvironment env = ...;
env.addSource(new TaxiRideSourceUDF())
   .map(new TaxiRideTupleUDF())
   .combiner(new TaxiRideCountSumUDF())
   .keyBy(new TaxiRideKeySelectorUDF())
   .reduce(new TaxiRideAvgReducerUDF())
   .sink(new PrintUDF());
```
### TPC-H benchmark query 01
This query is available at this [TPCHQuery01PreAggregate.java](flink-examples/flink-examples-streaming/src/main/java/org/apache/flink/streaming/examples/aggregate/TPCHQuery01PreAggregate.java).
```
./bin/flink run ../flink-applications/TPCHQuery01PreAggregate.jar -pre-aggregate-window-timeout 1 -controller false -slotSplit 2 -disableOperatorChaining true -input /home/flink/flink-applications/nycTaxiRides.gz -output mqtt -sinkHost IP
./bin/flink run ../flink-applications/TPCHQuery01PreAggregate.jar -pre-aggregate-window-timeout 1 -controller false -slotSplit 1 -parallelism-group-02 16 -disableOperatorChaining true -input /home/flink/flink-applications/nycTaxiRides.gz -output mqtt -sinkHost IP
./bin/flink run ../flink-applications/TPCHQuery01PreAggregate.jar -pre-aggregate-window-timeout 1 -controller false -slotSplit 1 -parallelism-group-02 24 -disableOperatorChaining true -input /home/flink/flink-applications/nycTaxiRides.gz -output mqtt -sinkHost IP

./bin/flink run ../flink-applications/TPCHQuery01PreAggregate.jar -controller true -slotSplit 2 -disableOperatorChaining true -input /home/flink/flink-applications/nycTaxiRides.gz -output mqtt -sinkHost IP
./bin/flink run ../flink-applications/TPCHQuery01PreAggregate.jar -controller true -slotSplit 1 -parallelism-group-02 16 -disableOperatorChaining true -input /home/flink/flink-applications/nycTaxiRides.gz -output mqtt -sinkHost IP
./bin/flink run ../flink-applications/TPCHQuery01PreAggregate.jar -controller true -slotSplit 1 -parallelism-group-02 24 -disableOperatorChaining true -input /home/flink/flink-applications/nycTaxiRides.gz -output mqtt -sinkHost IP
```
```
StreamExecutionEnvironment env = ...;
env.addSource(new LineItemSourceUDF())
   .map(new LineItemToTupleUDF())
   .combiner(new LineItemCountSumUDF())
   .keyBy(new LineItemKeySelectorUDF())
   .reduce(new LineItemAvgReducerUDF())
   .sink(new PrintUDF());
```

Checkout the [Flink dashboard](http://127.0.0.1:8081/) and the [Grafana dashboard](http://127.0.0.1:3000/).

## Autonomous Combiner operator
 - The example is implemented on the file [WordCountPreAggregate](flink-examples/flink-examples-streaming/src/main/java/org/apache/flink/streaming/examples/aggregate/WordCountPreAggregate.java).
 - This example aims to solve the issue [FLINK-7561: Add support for pre-aggregation in DataStream API](https://issues.apache.org/jira/browse/FLINK-7561).
 - We implemented a static and a dynamic pre-aggregator.

```
DataStream<String> text = env.fromElements(WordCountCombinerData.WORDS_SKEW)
	.flatMap(new Tokenizer())
	.combiner(new PreAggregateFunction())
	.keyBy(0)
	.sum(1)
	.print();
```

## Metrics

 - Job: [http://127.0.0.1:8081/jobs/<JOB_ID>](http://127.0.0.1:8081/jobs/<JOB_ID>)
 - Metrics: [http://127.0.0.1:8081/jobs/<JOB_ID>/metrics](http://127.0.0.1:8081/jobs/<JOB_ID>/metrics)
 - 99th percentile latency: [http://127.0.0.1:8081/jobs/<JOB_ID>/metrics?get=latency.source_id.<ID>.operator_id.<UID>.operator_subtask_index.0.latency_p99](http://127.0.0.1:8081/jobs/<JOB_ID>/metrics?get=latency.source_id.<ID>.operator_id.<UID>.operator_subtask_index.0.latency_p99). Detail description is given at [Latency tracking](https://ci.apache.org/projects/flink/flink-docs-stable/monitoring/metrics.html#latency-tracking).
 - For latency histogram of the pre-aggregate operator use the metric: `flink_taskmanager_job_task_operator_pre_aggregate_histogram`.
 - Backpressure: [http://localhost:8081/jobs/:jobID/vertices/:verticeID/backpressure](http://localhost:8081/jobs/:jobID/vertices/:verticeID/backpressure)

## Troubleshooting
```
$ git remote -v
origin  git@github.com:felipegutierrez/flink.git (fetch)
origin  git@github.com:felipegutierrez/flink.git (push)
upstream        https://github.com/apache/flink.git (fetch)
upstream        https://github.com/apache/flink.git (push)
// From original github repository
$ git fetch upstream
$ git merge upstream/master
// From my own github repository
$ git pull
// Open IntelliJ, update the project, and commit the new changes on my local repository
```

classes changed:
```
org.apache.flink.api.java.typeutils.TypeExtractor.java
org.apache.flink.metrics.MetricGroup.java
org.apache.flink.runtime.jobmaster.JobManagerRunnerImpl.java
org.apache.flink.runtime.jobmaster.factories.JobMasterServiceFactory.java
org.apache.flink.streaming.api.operators.StreamingRuntimeContext.java
org.apache.flink.streaming.api.datastream.DataStream.java
org.apache.flink.api.common.functions.PreAggregateConcurrentFunction;
org.apache.flink.api.common.functions.PreAggregateFunction;
org.apache.flink.streaming.api.functions.aggregation.PreAggregateTriggerFunction;
org.apache.flink.streaming.api.operators.StreamPreAggregateConcurrentOperator;
org.apache.flink.streaming.api.operators.StreamPreAggregateOperator;
org.apache.flink.streaming.api.operators.AbstractUdfStreamPreAggregateOperator;
org.apache.flink.metrics.groups.UnregisteredMetricsGroup;
AbstractMetricGroup
ProxyMetricGroup
```


