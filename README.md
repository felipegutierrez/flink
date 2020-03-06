# Apache Flink

This version of Apache Flink is forked from the original github repository and it is updated by:
```
$ git remote -v
origin	git@github.com:felipegutierrez/flink.git (fetch)
origin	git@github.com:felipegutierrez/flink.git (push)
upstream	https://github.com/apache/flink.git (fetch)
upstream	https://github.com/apache/flink.git (push)
// From original github repository
$ git remote fetch
// From my own github repository
$ git pull
```

## Compiling

```
$ cd flink-partition-tests/
$ mvn clean install -e -X -DskipTests -Dskip.npm -Dmaven.javadoc.skip=true -Dcheckstyle.skip=true \
    -Drat.skip=true
$ mvn clean install -DskipTests -Dskip.npm -Dfast
$ ll flink-examples/flink-examples-streaming/target/
```

## Quick start

To put the application to run quickly and with pre-configured options we just need to run a producer, the stream application, and a consumer.
```
java -classpath /home/flink/flink-1.9.0-partition/lib/flink-dist_2.11-1.10.jar:MqttDataProducer.jar \
    org.apache.flink.streaming.examples.utils.MqttDataProducer \
    -input /home/felipe/Temp/1524-0.txt -output mqtt

./bin/flink run WordCountPreAggregate.jar \
    -pre-aggregate-window 10 -input mqtt -sourceHost 192.168.56.1 \
    -output mqtt -sinkHost 192.168.56.1 -slotSplit true -disableOperatorChaining true -latencyTrackingInterval 10000
mosquitto_sub -h 192.168.56.1 -t topic-data-sink
```
You just start a producer that emits data every 10 seconds and the pre-aggregate stream application that pre-aggregate every 1000 milliseconds. In norder to chacke that the producer is actually producing data you can verify its mqtt broker topic. Then you can change its interval to produce data for 1 second. Its interval is defined in milliseconds (1000 milliseconds).
```
mosquitto_sub -h 127.0.0.1 -p 1883 -t topic-data-source
mosquitto_pub -h 127.0.0.1 -p 1883 -t topic-frequency-data-source -m "1000"
```
Checkout the [Flink dashboard](http://127.0.0.1:8081/) and the [Grafana dashboard](http://127.0.0.1:3000/).


## Options

You have the option to use which file you want to the stream application or some pre-defined files which follow some specific distribution
```
java -classpath ...MqttDataProducer -input [hamlet|mobydick|dictionary|your_file] -output mqtt
./bin/flink run ...WordCountPreAggregate.jar \
        -pre-aggregate-window [>0 seconds] \
        -input [mqtt|hamlet|mobydick|dictionary|words|skew|few|variation] \
        -sourceHost [127.0.0.1] -sourcePort [1883] \
        -pooling 100 \ # pooling frequency from source if not using mqtt data source
        -output [mqtt|log|text] \
        -sinkHost [127.0.0.1] -sinkPort [1883] \
        -slotSplit [false] -disableOperatorChaining [false] \
        -window [>=0 seconds] \
        -latencyTrackingInterval [0]
```







## Pre-aggregate operator
 - The example is implemented on the file [WordCountPreAggregate](flink-examples/flink-examples-streaming/src/main/java/org/apache/flink/streaming/examples/aggregate/WordCountPreAggregate.java).
 - This example aims to solve the issue [FLINK-7561: Add support for pre-aggregation in DataStream API](https://issues.apache.org/jira/browse/FLINK-7561).
 - We implemented a static and a dynamic pre-aggregator.

```
DataStream<String> text = env.fromElements(WordCountCombinerData.WORDS_SKEW)
	.flatMap(new Tokenizer())
	.preAggregate(new PreAggregateFunction(), 10)
	.keyBy(0)
	.sum(1)
	.print();
```
Then you can change the number of items that the Pre-aggregate operator is aggregating in run-time (without restarting the job).
```
mosquitto_pub -h 127.0.0.1 -p 1883 -t topic-frequency-pre-aggregate -m "100"
mosquitto_pub -h 127.0.0.1 -p 1883 -t topic-frequency-pre-aggregate -m "1000"
```

## Metrics

 - Job: [http://127.0.0.1:8081/jobs/<JOB_ID>](http://127.0.0.1:8081/jobs/<JOB_ID>)
 - Metrics: [http://127.0.0.1:8081/jobs/<JOB_ID>/metrics](http://127.0.0.1:8081/jobs/<JOB_ID>/metrics)
 - 99th percentile latency: [http://127.0.0.1:8081/jobs/<JOB_ID>/metrics?get=latency.source_id.<ID>.operator_id.<UID>.operator_subtask_index.0.latency_p99](http://127.0.0.1:8081/jobs/<JOB_ID>/metrics?get=latency.source_id.<ID>.operator_id.<UID>.operator_subtask_index.0.latency_p99). Detail description is given at [Latency tracking](https://ci.apache.org/projects/flink/flink-docs-stable/monitoring/metrics.html#latency-tracking).
 - For latency histogram of the pre-aggregate operator use the metric: `flink_taskmanager_job_task_operator_pre_aggregate_histogram`.
 - Backpressure: [http://localhost:8081/jobs/:jobID/vertices/:verticeID/backpressure](http://localhost:8081/jobs/:jobID/vertices/:verticeID/backpressure)

## Partial partition

 - The example is implemented on the file [WordCountPartitioning](flink-examples/flink-examples-streaming/src/main/java/org/apache/flink/streaming/examples/partitioning/WordCountPartitioning.java).
 - Sometimes the caniuse-lite is outdated and you will have to run the command `npm update` in another terminal.
 - Use `mvn clean package -DskipTests -Dcheckstyle.skip` to compile this project on the terminal or import it into the Intellij IDEA.
 - In order to run the Flink standalone cluster with this new version you have just to replace the file `lib/flink-dist_2.11-1.9.0.jar` to the new file `lib/flink-dist_2.11-1.10.jar` on the master and worker nodes. The new jar is located at `flink/flink-dist/target/flink-dist_2.11-1.10-SNAPSHOT.jar`.
 - Deploy the application using the following command: `./bin/flink run examples/streaming/WordCountPartitioning.jar -partition [rebalance|broadcast|rescale|forward|shuffle|global|power|''] &`.
 - Instruction on the [official Flink sodumentation](https://ci.apache.org/projects/flink/flink-docs-stable/flinkDev/ide_setup.html).
 - Implementing the [Power of both choices](https://melmeric.files.wordpress.com/2014/11/the-power-of-both-choices-practical-load-balancing-for-distributed-stream-processing-engines.pdf) in Flink.
 - Compile only the package necessary to deploy:


