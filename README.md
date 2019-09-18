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

 - The example is implemented on the file [WordCountPartitioning](felipegutierrez/flink/blob/master/flink-examples/flink-examples-streaming/src/main/java/org/apache/flink/streaming/examples/partitioning/WordCountPartitioning.java).
 - Sometimes the caniuse-lite is outdated and you will have to run the command `npm update` in another terminal.
 - Use `mvn clean package -DskipTests -Dcheckstyle.skip` to compile this project on the terminal or import it into the Intellij IDEA.
 - In order to run the Flink standalone cluster with this new version you have just to replace the file `lib/flink-dist_2.11-1.9.0.jar` to the new file `lib/flink-dist_2.11-1.10.jar` on the master and worker nodes. The new jar is located at `flink/flink-dist/target/flink-dist_2.11-1.10-SNAPSHOT.jar`.
 - Deploy the application using the following command: `./bin/flink run examples/streaming/WordCountPartitioning.jar -partition [rebalance|broadcast|rescale|forward|shuffle|global|power|''] &`.
 - Instruction on the [official Flink sodumentation](https://ci.apache.org/projects/flink/flink-docs-stable/flinkDev/ide_setup.html).
 - Implementing the [Power of both choices](https://melmeric.files.wordpress.com/2014/11/the-power-of-both-choices-practical-load-balancing-for-distributed-stream-processing-engines.pdf) in Flink.
 - Compile only the package necessary to deploy:
```
$ cd flink-partition-tests/
$ mvn clean install -DskipTests -Dskip.npm -Dmaven.javadoc.skip=true -Dcheckstyle.skip=true -Drat.skip=true
$ mvn clean install -DskipTests -Dskip.npm -Dfast
$ ll flink-examples/flink-examples-streaming/target/
```


## Original README.md file

Apache Flink is an open source stream processing framework with powerful stream- and batch-processing capabilities.

Learn more about Flink at [https://flink.apache.org/](https://flink.apache.org/)


### Features

* A streaming-first runtime that supports both batch processing and data streaming programs

* Elegant and fluent APIs in Java and Scala

* A runtime that supports very high throughput and low event latency at the same time

* Support for *event time* and *out-of-order* processing in the DataStream API, based on the *Dataflow Model*

* Flexible windowing (time, count, sessions, custom triggers) across different time semantics (event time, processing time)

* Fault-tolerance with *exactly-once* processing guarantees

* Natural back-pressure in streaming programs

* Libraries for Graph processing (batch), Machine Learning (batch), and Complex Event Processing (streaming)

* Built-in support for iterative programs (BSP) in the DataSet (batch) API

* Custom memory management for efficient and robust switching between in-memory and out-of-core data processing algorithms

* Compatibility layers for Apache Hadoop MapReduce

* Integration with YARN, HDFS, HBase, and other components of the Apache Hadoop ecosystem


### Streaming Example
```scala
case class WordWithCount(word: String, count: Long)

val text = env.socketTextStream(host, port, '\n')

val windowCounts = text.flatMap { w => w.split("\\s") }
  .map { w => WordWithCount(w, 1) }
  .keyBy("word")
  .timeWindow(Time.seconds(5))
  .sum("count")

windowCounts.print()
```

### Batch Example
```scala
case class WordWithCount(word: String, count: Long)

val text = env.readTextFile(path)

val counts = text.flatMap { w => w.split("\\s") }
  .map { w => WordWithCount(w, 1) }
  .groupBy("word")
  .sum("count")

counts.writeAsCsv(outputPath)
```



## Building Apache Flink from Source

Prerequisites for building Flink:

* Unix-like environment (we use Linux, Mac OS X, Cygwin)
* Git
* Maven (we recommend version 3.2.5 and require at least 3.1.1)
* Java 8 (Java 9 and 10 are not yet supported)

```
git clone https://github.com/apache/flink.git
cd flink
mvn clean package -DskipTests # this will take up to 10 minutes
```

Flink is now installed in `build-target`

*NOTE: Maven 3.3.x can build Flink, but will not properly shade away certain dependencies. Maven 3.1.1 creates the libraries properly.
To build unit tests with Java 8, use Java 8u51 or above to prevent failures in unit tests that use the PowerMock runner.*

## Developing Flink

The Flink committers use IntelliJ IDEA to develop the Flink codebase.
We recommend IntelliJ IDEA for developing projects that involve Scala code.

Minimal requirements for an IDE are:
* Support for Java and Scala (also mixed projects)
* Support for Maven with Java and Scala


### IntelliJ IDEA

The IntelliJ IDE supports Maven out of the box and offers a plugin for Scala development.

* IntelliJ download: [https://www.jetbrains.com/idea/](https://www.jetbrains.com/idea/)
* IntelliJ Scala Plugin: [http://plugins.jetbrains.com/plugin/?id=1347](http://plugins.jetbrains.com/plugin/?id=1347)

Check out our [Setting up IntelliJ](https://ci.apache.org/projects/flink/flink-docs-master/flinkDev/ide_setup.html#intellij-idea) guide for details.

### Eclipse Scala IDE

**NOTE:** From our experience, this setup does not work with Flink
due to deficiencies of the old Eclipse version bundled with Scala IDE 3.0.3 or
due to version incompatibilities with the bundled Scala version in Scala IDE 4.4.1.

**We recommend to use IntelliJ instead (see above)**

## Support

Don’t hesitate to ask!

Contact the developers and community on the [mailing lists](https://flink.apache.org/community.html#mailing-lists) if you need any help.

[Open an issue](https://issues.apache.org/jira/browse/FLINK) if you found a bug in Flink.


## Documentation

The documentation of Apache Flink is located on the website: [https://flink.apache.org](https://flink.apache.org)
or in the `docs/` directory of the source code.


## Fork and Contribute

This is an active open-source project. We are always open to people who want to use the system or contribute to it.
Contact us if you are looking for implementation tasks that fit your skills.
This article describes [how to contribute to Apache Flink](https://flink.apache.org/contributing/how-to-contribute.html).


## About

Apache Flink is an open source project of The Apache Software Foundation (ASF).
The Apache Flink project originated from the [Stratosphere](http://stratosphere.eu) research project.
