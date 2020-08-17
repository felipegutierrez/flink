# Apache Flink

This version of Apache Flink is forked from the original github repository and it is updated by:
```
$ git remote -v
origin	git@github.com:felipegutierrez/flink.git (fetch)
origin	git@github.com:felipegutierrez/flink.git (push)
upstream	https://github.com/apache/flink.git (fetch)
upstream	https://github.com/apache/flink.git (push)
// From original github repository
$ git fetch upstream
$ git merge upstream/master
// From my own github repository
$ git pull
// Open IntelliJ, update the project, and commit the new changes on my local repository
```

## Compiling

```
$ cd flink-partition-tests/
$ mvn clean install -e -X -DskipTests -Dskip.npm -Dmaven.javadoc.skip=true -Dcheckstyle.skip=true -Drat.skip=true
$ mvn clean install -DskipTests -Dskip.npm -Dfast
$ ll flink-examples/flink-examples-streaming/target/
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
Running static combiner with 8 combiners and 8-16-24 reducers:
```
./bin/flink run ../flink-applications/TaxiRideCountPreAggregate.jar -pre-aggregate-window-timeout 1 -controller false -slotSplit 2 -disableOperatorChaining true -input /home/flink/flink-applications/nycTaxiRides.gz -output mqtt -sinkHost IP_r02
```
Running autonomous combiner with 8 combiners and 8-16-24 reducers:
```
./bin/flink run ../flink-applications/TaxiRideCountPreAggregate.jar -controller true -slotSplit 2 -disableOperatorChaining true -input /home/flink/flink-applications/nycTaxiRides.gz -output mqtt -sinkHost IP_r02
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
