# To run the code

## 1. Download the Tippers Sample data

##### Download
`cs237_GeneralEventingSystem $ scp cs237@128.195.52.128:~/cs237.db.zip ./`

password: cs237.

*Note: This cs237.db.zip file is about 1.1GB, will take a while to download it*

##### Unzip
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

It means: Once the TippersAdapter starts, any client connects to 
it can get the streaming data from Tippers database since `Nov 08, 2017`,
 and every 5 seconds, it will get the next 1 minute new data.

 
```
cs237_GeneralEventingSystem $ mvn clean package
cs237_GeneralEventingSystem $ mvn exec:java -Dexec.mainClass="cs237.TippersAdapter"
```

## 3. Run the SparkAppJava

```
spark-submit --class cs237.SparkAppJava --master local[2] target/cs237-project-1.0-SNAPSHOT.jar localhost 9999
```

Every 5 seconds, you will see something like this:

```
...
-------------------------------------------
Time: 1527572195000 ms
-------------------------------------------
(a0591d80_b6ee_4db4_a7e5_913cf0f2003e-2017-11-08 00:30:00,34)
(48ec9043_4d33_4fc5_b79d_62eca3864f74-2017-11-08 00:30:00,31)
(eb5c404c_3456_4041_841f_16347a493d36-2017-11-08 00:30:00,10)
(cf4e7edc_33fd_4112_b8dd_d1d098712eaf-2017-11-08 00:30:00,44)
(29a9e39e_f73b_441c_9709_71b9f90b54df-2017-11-08 00:30:00,42)
(8d8cc3cb_87cd_49b5_9770_1786f8fd8170-2017-11-08 00:30:00,44)
(da10f8a6_1382_460a_943b_8c2383f67779-2017-11-08 00:30:00,60)
(6ed36721_63d2_48b3_af93_0066ffb20308-2017-11-08 00:30:00,84)
(afa49397_9b8b_4468_8d26_189e95ae819a-2017-11-08 00:30:00,98)
(5ba0167e_a57f_445f_b734_6f6c72231bcc-2017-11-08 00:30:00,56)
...

```

Currently it will output 10 records of the ThermometerObservation 
in this format `(sensorID-Timestamp, value)` 
with the predicate: `Thermometer value >= 80`

