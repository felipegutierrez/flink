#!/usr/bin/env bash

# Flink libraries
cp /home/felipe/workspace-idea/flink-partition-tests/flink-dist/target/flink-1.12-SNAPSHOT-bin/flink-1.12-SNAPSHOT/lib/flink-csv-1.12-SNAPSHOT.jar lib/flink-csv-1.11.2.jar
cp /home/felipe/workspace-idea/flink-partition-tests/flink-dist/target/flink-1.12-SNAPSHOT-bin/flink-1.12-SNAPSHOT/lib/flink-dist_2.12-1.12-SNAPSHOT.jar lib/flink-dist_2.12-1.11.2.jar
cp /home/felipe/workspace-idea/flink-partition-tests/flink-dist/target/flink-1.12-SNAPSHOT-bin/flink-1.12-SNAPSHOT/lib/flink-json-1.12-SNAPSHOT.jar lib/flink-json-1.11.2.jar
cp /home/felipe/workspace-idea/flink-partition-tests/flink-dist/target/flink-1.12-SNAPSHOT-bin/flink-1.12-SNAPSHOT/lib/flink-shaded-zookeeper-3.4.14.jar lib/flink-shaded-zookeeper-3.4.14.jar
cp /home/felipe/workspace-idea/flink-partition-tests/flink-dist/target/flink-1.12-SNAPSHOT-bin/flink-1.12-SNAPSHOT/lib/flink-table-blink_2.12-1.12-SNAPSHOT.jar lib/flink-table-blink_2.12-1.11.2.jar
cp /home/felipe/workspace-idea/flink-partition-tests/flink-dist/target/flink-1.12-SNAPSHOT-bin/flink-1.12-SNAPSHOT/lib/flink-table_2.12-1.12-SNAPSHOT.jar lib/flink-table_2.12-1.11.2.jar

# Flink applications
cp /home/felipe/workspace-idea/flink-partition-tests/flink-examples/flink-examples-streaming/target/flink-examples-streaming_2.12-1.12-SNAPSHOT-WordCount.jar ../flink-app/
# Flink using AdCom
cp /home/felipe/workspace-idea/flink-partition-tests/flink-adcom/target/flink-adcom_2.12-1.12-SNAPSHOT-TaxiRideCountPreAggregate.jar ../flink-app/
cp /home/felipe/workspace-idea/flink-partition-tests/flink-adcom/target/flink-adcom_2.12-1.12-SNAPSHOT-TaxiRideDistanceAveragePreAggregate.jar ../flink-app/
# Flink using Table API
cp /home/felipe/workspace-idea/flink-partition-tests/flink-adcom/target/flink-adcom_2.12-1.12-SNAPSHOT-TaxiRideCountTablePreAggregate.jar ../flink-app/


# Executing Flink applications
# ./bin/flink run ../flink-app/flink-examples-streaming_2.12-1.12-SNAPSHOT-WordCount.jar --input /home/felipe/Temp/1524-0-4.txt

# TaxiRide pre-aggregation AdCom
# ./bin/flink run ../flink-app/flink-adcom_2.12-1.12-SNAPSHOT-TaxiRideCountPreAggregate.jar -controller false -pre-aggregate-window-timeout 2000 -disableOperatorChaining true -input /home/flink/nycTaxiRides.gz -input-par true -output mqtt -sinkHost 127.0.0.1
# ./bin/flink run ../flink-app/flink-adcom_2.12-1.12-SNAPSHOT-TaxiRideCountPreAggregate.jar -controller true -pre-aggregate-window-timeout 500 -disableOperatorChaining true -input /home/flink/nycTaxiRides.gz -input-par true -output mqtt -sinkHost 127.0.0.1

# TaxiRide pre-aggregation Table API
# ./bin/flink run ../flink-app/flink-adcom_2.12-1.12-SNAPSHOT-TaxiRideCountTablePreAggregate.jar -input /home/flink/nycTaxiRides.gz -input-par true -output mqtt -sinkHost 127.0.0.1 -mini_batch_enabled true -mini_batch_latency 1_s -mini_batch_size 1000 -mini_batch_two_phase true -parallelism-table 4


# Changing the data rate
# echo '1000000000' > /tmp/datarate.txt    # 1    rec/sec
# echo '2000000' > /tmp/datarate.txt       # 500  rec/sec
# echo '1000000' > /tmp/datarate.txt       # 1K   rec/sec
# echo '200000' > /tmp/datarate.txt        # 5K   rec/sec
# echo '100000' > /tmp/datarate.txt        # 10K  rec/sec
# echo '66666' > /tmp/datarate.txt         # 15K  rec/sec
# echo '50000' > /tmp/datarate.txt         # 20K  rec/sec
# echo '20000' > /tmp/datarate.txt         # 50K  rec/sec
# echo '10000' > /tmp/datarate.txt         # 100K rec/sec
# echo '5000' > /tmp/datarate.txt          # 200K rec/sec
# echo '2000' > /tmp/datarate.txt          # 500K rec/sec
# echo '1000' > /tmp/datarate.txt          # 1M   rec/sec
# echo '500' > /tmp/datarate.txt           # 2M   rec/sec



