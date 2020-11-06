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
