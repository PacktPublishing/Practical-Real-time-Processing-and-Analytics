How to execute chapter 7 diy code:
	1 - Create topic on Kafka
	    ./kafka-topics.sh --create --topic storm-diy --zookeeper localhost:2181 --partitions 1 --replication-factor 1
	2 - Create keyspace and tables in Cassandra
		CREATE KEYSPACE usage WITH replication = {'class':'SimpleStrategy', 'replication_factor' : 3};
		CREATE KEYSPACE tdr WITH replication = {'class':'SimpleStrategy', 'replication_factor' : 3};
		create table uasge.packet_usage (phone_number bigint PRIMARY KEY, bin int, bout int, total_bytes int);
		create table tdr.packet_tdr (phone_number bigint PRIMARY KEY, bin int, bout int, timestamp varchar);
	3 - Run HCServer.java to start HC server.
	4 - Run DataGenerator.java to generate data in kafka topic. It takes an argument as number of records to be generated.
	5 - Run TelecomProcessorTopology.java

How to check the values in Cassandra:
	1- Execute commands:
		./csql
		select * from usage.packet_usage;
		select * from tdr.packet_tdr;
		They will show fist 15 records only.