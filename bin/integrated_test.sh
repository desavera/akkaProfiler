 2102  bin/kafka-console-producer.sh --broker-list localhost:9092 --topic example_topic
 2103  bin/kafka-run-class.sh kafka.tools.DumpLogSegments --deep-iteration --print-data-log --files /tmp/kafka-logs/example_topic-0/00000000000000000000.log 
