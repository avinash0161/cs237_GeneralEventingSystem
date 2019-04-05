Created by: Qiushi Bai, Jonathan Harijanto, and Avinash Kumar

# To run the code

## 0. Prerequisites

##### (1) Spark 2.3.0

- Download

https://www.apache.org/dyn/closer.lua/spark/spark-2.3.0/spark-2.3.0-bin-hadoop2.7.tgz

- Unzip

```
# pwd
/usr/local
# tar -zxvf spark-2.3.0-bin-hadoop2.7.tgz
```

- Configure PATH

`# vi .bash_profile`

Add one line:

`export PATH=$PATH:/usr/local/spark-2.3.0-bin-hadoop2.7/bin`

Save file

- Configure Spark Log level to ERROR (to minimize the log output info so we can see our code's output easily.)

```
# pwd
/usr/local/spark-2.3.0-bin-hadoop2.7/conf
# vi vi log4j.properties
Make this log4j conf to be ERROR level with console output by changing this line to :
log4j.rootCategory=ERROR, console, RollingAppender
```

##### (2) SQLite3

Usually it's built-in any Linux based systems, no need to install explicitly.

To test: 

```
# sqlite3
SQLite version 3.19.3 2017-06-27 16:48:08
Enter ".help" for usage hints.
Connected to a transient in-memory database.
Use ".open FILENAME" to reopen on a persistent database.
sqlite>.quit
```

##### (3) Maven

https://maven.apache.org/install.html

Mac: `brew install maven`

Ubuntu: `sudo apt-get install maven`


## 1. Download the Tippers Sample data

##### (1.1) Download

`cs237_GeneralEventingSystem $ scp cs237@128.195.52.128:~/cs237.db.zip ./`

*Note: This cs237.db.zip file is about 1.1GB, will take a while to download it*

##### (1.2) Unzip

`cs237_GeneralEventingSystem $ unzip cs237.db.zip`


## 2. Run the TippersAdapter

This adapter will pull the real time data from Tippers Database
and serve as Socket server for clients to connect to and stream this data.

For experiments purpose, we cannot get real world real-time data 
in a really real-time manner, this adapter can also plays a role like 
a simulator who simulates every `tickFrequency`(e.g. 5) seconds 
and pull the Tippers data recent `tickFrequency * frequencyScale`
(e.g. 5*12) seconds. Of course the start date of this simulation data 
is not today, it's `Nov 08, 2017`.

It means: Once the TippersAdapter starts, it will start a Socket server
at `localhost:9999` port. Any client connects to it can get the 
streaming data from Tippers database since `Nov 08, 2017`,
 and every 5 seconds, it will get the next 1 minute new data.


Note: Open a terminal, don't close it. 
```
cs237_GeneralEventingSystem $ mvn clean compile assembly:single
cs237_GeneralEventingSystem $ mvn exec:java -Dexec.mainClass="cs237.TippersAdapter"
```


## 3. Run the Apache ActiveMQ

For our event framework, we use Apache ActiveMQ (http://activemq.apache.org/) 
as the Pub/Sub messaging middleware. 

##### (3.1) Download 

http://activemq.apache.org/activemq-5154-release.html

##### (3.2) Unzip

`tar -zxvf apache-activemq-5.15.4-bin.tar.gz`

##### (3.3) Start

```
cd apache-activemq-5.15.4/bin
./activemq start
```


## 4. Run Subscriber

This Subscriber is an example code to simulate a real application who will always
 listen to the topic it interests in.
 
This example Subscriber is now playing the role of two real subscribers, one of them
listening to Event topic `test-TwoUserPresentSameFloor` and the other listening to 
Event topic `test-UserInBuildingAndRoomEmpty`. These two topics are actually the two
example event rules that registered into our Event Framework.

Once it gets some event message from ActiveMQ Pub/Sub service, the Subscriber outputs
the message to console.

Note: Open a new Terminal, don't close it.

`cs237_GeneralEventingSystem $ mvn exec:java -Dexec.mainClass="cs237.Subscriber"`


## 5. Run the Event Framework

This is our core logic of this project. Our Event Framework is a Spark Streaming Job 
who will listen to the `localhost:9999` port and process all the streaming data received
from this port.

The processing includes the following 6 steps:

 - (1) Load the Event Rules defined by the user applications.
 - (2) Map the whole stream to different data source streams.
 - (3) Apply the Event Rules' predicates to each record in each stream.
 - (4) Reduce the predicate-mapping-to-records of each Rule. 
 - (5) Apply the Merger functions defined by Event Rules to all the predicate-mapping-to-records for each Rule.
 - (6) Publish the records to Apache ActiveMQ with topicId = RuleId.

Note: Open a new Terminal, don't close it.
```
cs237_GeneralEventingSystem $ spark-submit --class cs237.SparkAppJava --master local[2] target/cs237-project-1.0-SNAPSHOT-jar-with-dependencies.jar 
```

You will see output from Event Framework running like following:
(The ERROR messages are intentionally output for the convenience of 
differentiating our codes' output from other Spark normal messages.)

```
...
18/06/11 11:55:45 ERROR root: ========== Record : [PRESENCE, ea557fc5-2fc8-4918-9ce8-e709de03b5d2, user8, 4100_5, 2017-11-08 07:02:00, vSensor1] <<<<<< Rule: r_user_in_building
18/06/11 11:55:50 ERROR root: ========== Record : [PRESENCE, ea557fc5-2fc8-4918-9ce8-e709de03b5d2, user8, 4100_5, 2017-11-08 07:02:00, vSensor1] <<<<<< Rule: r_user_in_building
18/06/11 11:55:50 ERROR root: ========== Record : [PRESENCE, 9dc01361-d8e6-4562-83d0-f1872a441c41, user8, 2058, 2017-11-08 07:04:00, vSensor1] <<<<<< Rule: r_user1_on_floor
18/06/11 11:55:50 ERROR root: ========== Record : [PRESENCE, 9dc01361-d8e6-4562-83d0-f1872a441c41, user8, 2058, 2017-11-08 07:04:00, vSensor1] <<<<<< Rule: r_user_in_building
18/06/11 11:55:55 ERROR root: ========== Record : [PRESENCE, 9dc01361-d8e6-4562-83d0-f1872a441c41, user8, 2058, 2017-11-08 07:04:00, vSensor1] <<<<<< Rule: r_user1_on_floor
18/06/11 11:55:55 ERROR root: ========== Record : [PRESENCE, 9dc01361-d8e6-4562-83d0-f1872a441c41, user8, 2058, 2017-11-08 07:04:00, vSensor1] <<<<<< Rule: r_user_in_building
18/06/11 11:55:55 ERROR root: ========== Record : [PRESENCE, a5905739-d2a8-4906-a0bb-2b320678a0d8, user9, 2056, 2017-11-08 07:06:00, vSensor1] <<<<<< Rule: r_user2_on_floor
18/06/11 11:55:55 ERROR root: ========== Publisher is publishing message: [PRESENCE|9dc01361-d8e6-4562-83d0-f1872a441c41|user8|2058|2017-11-08 07:04:00|vSensor1] to topic: [test-TwoUserPresentSameFloor].
18/06/11 11:55:55 ERROR root: ========== POST Response Code :: 200
18/06/11 11:55:55 ERROR root: ========== [Publisher] POST Succeed!!!
18/06/11 11:55:55 ERROR root: ========== POST Response :: Message sent
18/06/11 11:55:55 ERROR root: ========== Publisher is publishing message: [PRESENCE|a5905739-d2a8-4906-a0bb-2b320678a0d8|user9|2056|2017-11-08 07:06:00|vSensor1] to topic: [test-TwoUserPresentSameFloor].
18/06/11 11:55:55 ERROR root: ========== POST Response Code :: 200
18/06/11 11:55:55 ERROR root: ========== [Publisher] POST Succeed!!!
18/06/11 11:55:55 ERROR root: ========== POST Response :: Message sent
18/06/11 11:55:55 ERROR root: ========== Published 2 messages to topic: test-TwoUserPresentSameFloor

...

```

At the same time you will see output from Subscriber's terminal that the event messages we published:

```
...
========== [Message Received] Topic : test-TwoUserPresentSameFloor --------> Message: PRESENCE|9dc01361-d8e6-4562-83d0-f1872a441c41|user8|2058|2017-11-08 07:04:00|vSensor1
========== [Message Received] Topic : test-TwoUserPresentSameFloor --------> Message: PRESENCE|a5905739-d2a8-4906-a0bb-2b320678a0d8|user9|2056|2017-11-08 07:06:00|vSensor1
...
```

