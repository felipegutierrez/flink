# Apache Flink with AdCom

This is an [Apache Flink](https://github.com/apache/flink) branch forked from the original repository of Apache Flink version 1.11.2 which has an adaptive combiner (AdCom) for stream aggregations.

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

## Using

One can use AdCom by calling the operator `adCombine(PreAggregateFunction())` which starts AdCom with 500 milliseconds and adapts the pre-aggregation during runtime. A static combine is also available by calling the `combine(PreAggregateFunction(), time in milliseconds)` operator which pre-aggregates win window times passed as parameter.

```
StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

// using the static combiner
env.addSource(new TaxiRideSource())
        .combine(taxiRidePreAggregateFunction, genericParam.getPreAggregationProcessingTimer())
        .keyBy(new TaxiRideKeySelector())
        .reduce(new TaxiRideSumReduceFunction())
        .print();
        
// using the adaptive combiner with the PI controller
env.addSource(new TaxiRideSource())
        .adCombine(taxiRidePreAggregateFunction)
        .keyBy(new TaxiRideKeySelector())
        .reduce(new TaxiRideSumReduceFunction())
        .print();
```

## Troubleshooting
```
$ git remote -v
origin	git@github.com:felipegutierrez/flink-adcom.git (fetch)
origin	git@github.com:felipegutierrez/flink-adcom.git (push)
upstream	https://github.com/apache/flink.git (fetch)
upstream	https://github.com/apache/flink.git (push)

// Pull code from original github repository
$ git fetch upstream
// Fix issues and merge
$ git merge upstream/master

// pull code from flink-adcom github repository
$ git pull
```
