How to execute chapter 8 diy code:
	1 - Create topic on Kafka
	    ./kafka-topics.sh --create --topic storm-trident-diy --zookeeper localhost:2181 --partitions 1 --replication-factor 1
	2 - Run DataGenerator.java to generate data in kafka topic. It takes an argument as number of records to be generated.
	3 - Run TridentDIY.java