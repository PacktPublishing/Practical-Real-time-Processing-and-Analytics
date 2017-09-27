This will explain how to execute DIY examples

###Source to sink : Flink execution

Program to execute is FlinkESConnector.java. Below are steps to execute the program:

* Start Elastic Search 2.x
	install Elastic Search 2.2.1 from https://download.elasticsearch.org/elasticsearch/release/org/elasticsearch/distribution/tar/elasticsearch/2.2.1/elasticsearch-2.2.1.tar.gz
	Open elasticsearch.yml and give cluster name as "my-application"
	Start elasticseach

* Create Index on elastic search

>curl -XPUT 'localhost:9200/flink-test/_mapping/flink-log' -d '{
	  "properties": {
		    "phoneNumber": {
		      "type": "long",
		      "index": "not_analyzed"
		    },
		    "bin": {
		        "type": "integer",
		        "index": "not_analyzed"
		    },
		    "bout": {
		    	"type": "integer",
		    	"index": "not_analyzed"
		    },
		    "timestamp": {
		    	"type": "long",
		    	"index": "not_analyzed"
		    }
	  }
	}'

* Create document type:

>curl -XPUT 'http://localhost:9200/flink-test/' -d '{
     "settings" : {
         "index" : {
             "number_of_shards" : 1, 
             "number_of_replicas" : 0
         }
     }
	}'

* Check indexes:

>curl 'localhost:9200/_cat/indices?v'

* Check values:

>curl -XGET 'localhost:9200/flink-test/flink-log/_search?pretty'

* Start Kafka 0.8.2.2 
	Start kafka and create topic with command
 
> kafka-topics.sh --create --topic device-data --zookeeper localhost:2181 --partitions 1 --replication-factor 1

* Run program
	Right click on FlinkESConnector and select run as "Java Application". It will start processing the records.
	To push messages in kafka topic, run com.book.flink.diy.DataGenerator program with number of records as argument.
	Messages will start display in Elastic search.

###Executing storm topology on Flink

Program to execute is FlinkStormExample.java. Below are steps to execute the program:

* Create a text file at /tmp with file name as device-data.txt contains below records:

>	9999999966,7007359,9190386,1499855254522
	9999999973,4361953,691987,1499855254729
	9999999993,9473314,6547467,1499855254729
	9999999998,3615747,645638,1499855254729
	9999999983,7726351,1681259,1499855254730
	9999999957,3042432,2071504,1499855254730
	9999999958,5740147,3959380,1499855254730
	9999999974,6046098,979331,1499855254730
	9999999968,9702645,7537844,1499855254730
	9999999993,6678668,1406987,1499855254730
	9999999993,2652821,2193809,1499855254730
	9999999959,2101658,4378334,1499855254730
	9999999967,3183438,6646936,1499855254730
	9999999959,6758041,1557227,1499855254731
	9999999987,6712921,2515520,1499855254731
	9999999967,5758958,286631,1499855254731
	9999999975,8345966,7967788,1499855254731
	9999999995,6326700,5041383,1499855254731
	9999999962,1707666,6800374,1499855254748

* Start cassandra and create KeySpace and table as below:

>CREATE KEYSPACE tdr WITH replication = {'class':'SimpleStrategy', 'replication_factor' : 3};
	create table tdr.packet_tdr (phone_number bigint PRIMARY KEY, bin int, bout int, timestamp bigint);

* Run program

	Right click on FlinkStormExample and select run as "Java Application". It will start processing the records.

* Check records in Cassandra:

>select * from tdr.packet_tdr; 