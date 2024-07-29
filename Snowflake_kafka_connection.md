## Snowflake_kafka_connection

### Kafka Setup 

* apt-get update
* apt-get install openjdk-11-jdk -y
* export JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64
* export PATH=$JAVA_HOME/bin:$PATH
* wget https://downloads.apache.org/kafka/3.7.1/kafka_2.13-3.7.1.tgz
* tar -xzf kafka_2.13-3.7.1.tgz
* cd kafka_2.13-3.7.1


### Kafka Snowflake Integration:

Download the required jar file -- https://repo1.maven.org/maven2/com/snowflake/snowflake-kafka-connector/2.3.0/snowflake-kafka-connector-2.3.0.jar

Put this jar in libs folders

Update the plugin.path in kafka connect-standalone.properties file
eg : plugin.path=/kafka-3.7.1-src/kafka_2.13-3.7.1/libs



### connection and ssh key generation for snowflake

* openssl genrsa -out rsa_key.pem 2048
* openssl rsa -in rsa_key.pem -pubout -out rsa_key.pub

Update for the user in snowflake 
* alter user {User_name} set rsa_public_key='{Put the Public key content here}';
  
Verify the public key is configured properly or not --
* desc user {User_name};

### To unset the Public Key in Snowflake:
* alter user {User_name} unset rsa_public_key;

### SF_connection.properties files

create the SF_connection.properties file in the config directory as 
[SF_connect.properties](https://github.com/Krishna4802/Snowflake/blob/main/SF_connect.properties)

### starting kafka and zookeeper

*  bin/zookeeper-server-start.sh config/zookeeper.properties
*  bin/kafka-server-start.sh config/server.properties
  
*   bin/kafka-topics.sh --create --topic test --bootstrap-server localhost:9092 --replication-factor 1 --partitions 1
*   bin/kafka-topics.sh --list --bootstrap-server localhost:9092
*   ./bin/connect-standalone.sh config/connect-standalone.properties config/SF_connect.properties
