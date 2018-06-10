package cs237;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;

import com.google.common.io.Closeables;

import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.storage.StorageLevel;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.receiver.Receiver;
import scala.Tuple2;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.ConnectException;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.*;

public class SparkAppJava extends Receiver<String> {

    String streamFromHost = "localhost";
    int streamFromPort = 9999;

    // Metadata of Streams
    public static Map<String, Map<String, Integer>> streamMetaMap = new HashMap<>();

    // Event Rulls
    public static Map<String, IEventRule> ruleMap = new HashMap<>();
    public static Map<String, List<IRecordRule>> streamRecordRuleMap = new HashMap<>();

    public static Logger log = LogManager.getRootLogger();

    /**
     * Load Event Rules
     * TODO - (1) Load the rules list from some file or DB. (2) Also load the java objects from jar files.
     */
    public static void loadEventRules() {

        // (1) Load the Rules from somewhere.
//        IEventRule rule0 = new ThermometerGT80Rule();
//        ruleMap.put(rule0.ruleId(), rule0);
//        IEventRule rule1 = new TwoUserBothPresentSameRoom();
//        ruleMap.put(rule1.ruleId(), rule1);
        IEventRule rule2 = new TwoUserPresentSameFloor("user8", "user9", "20");
        ruleMap.put(rule2.ruleId(), rule2);
        IEventRule rule3 = new UserInBuildingAndRoomEmpty("user8", "2065");
        ruleMap.put(rule3.ruleId(), rule3);

        // (2) Construct the Map of stream to List of Predicates;
        ruleMap.forEach((ruleId, rule) -> {
            rule.recordRuleList().forEach(recordRule -> {
                if (streamRecordRuleMap.containsKey(recordRule.stream())) {
                     streamRecordRuleMap.get(recordRule.stream()).add(recordRule);
                }
                else {
                    streamRecordRuleMap.put(recordRule.stream(), new ArrayList<>(Arrays.asList(recordRule)));
                }
            });
        });
    }

    /**
     * Get Attribute Value of given Attribute Name of given Record
     *
     * @param record
     * @param attributeName
     * @return
     */
    public static String getAttributeValue(List<String> record, String attributeName) {
        String stream = record.get(0);

        Map<String, Integer> schema = streamMetaMap.get(stream);

        int indexOfId = schema.get(attributeName);

        return record.get(indexOfId);
    }

    /**
     * Apply a record rule on a record.
     * NOTE: Only support conjunction of all predicates of one record.
     * Example: Presence.semantic_entity_id = 'user8' and Presence.location = 2065
     *
     * @param record
     * @param recordRule
     * @return
     */
    public static boolean applyRecordRule(List<String> record, IRecordRule recordRule) {
        List<IPredicate> predicateList = recordRule.predicateList();

        // - DEBUG - //
//        log.info("========== Process Record: " + record);
//        System.out.println("========== Process Record: " + record);
        // - DEBUG - //

        for (Iterator<IPredicate> iter = predicateList.iterator(); iter.hasNext();) {
            IPredicate predicate = iter.next();
            String attributeValue = getAttributeValue(record, predicate.attribute());

            // - DEBUG - //
//            log.info("========== Predicate: " + predicate.id());
//            log.info("==========     Attribute: " + predicate.attribute());
//            log.info("==========     AttributeType: " + predicate.attributeType());
//            log.info("==========     Operator: " + predicate.operator());
//            log.info("==========     ValueString: " + predicate.valueString());
//            log.info("==========     ValueInt: " + predicate.valueInt());
//            log.info("==========     ValueString: " + predicate.valueFloat());
//            System.out.println("========== Predicate: " + predicate.id());
//            System.out.println("==========     Attribute: " + predicate.attribute());
//            System.out.println("==========     AttributeType: " + predicate.attributeType());
//            System.out.println("==========     Operator: " + predicate.operator());
//            System.out.println("==========     ValueString: " + predicate.valueString());
//            System.out.println("==========     ValueInt: " + predicate.valueInt());
//            System.out.println("==========     ValueString: " + predicate.valueFloat());
            // - DEBUG - //

            if (!applyPredicate(attributeValue, predicate)) {
                // - DEBUG - //
//                log.info("========== Predicate:" + predicate.id() + " [False] -NO-!");
//                System.out.println("========== Predicate:" + predicate.id() + " [False] -NO-!");
                // - DEBUG - //
                return false;
            }
        }

        // - DEBUG - //
        log.info("========== Record : " + record + " <<<<<< Rule: " + recordRule.id());
        System.out.println("========== Record : " + record + " <<<<<< Rule: " + recordRule.id());
//        log.info("========== Rule: " + recordRule.id() + " [True] ---YES!!!---");
//        System.out.println("========== Rule: " + recordRule.id() + " [True] ---YES!!!---");
        // - DEBUG - //

        return true;
    }

    /**
     * Apply a predicate on an attribute value
     *
     * @param attributeValue
     * @param predicate
     * @return
     */
    public static boolean applyPredicate(String attributeValue, IPredicate predicate) {

        switch(predicate.operator()) {
            case EQUAL:
                switch(predicate.attributeType()) {
                    case STRING:
                        return attributeValue.equals(predicate.valueString());
                    case INT:
                        return Integer.valueOf(attributeValue) == predicate.valueInt();
                    case FLOAT:
                        return Double.valueOf(attributeValue) == predicate.valueFloat();
                }
                break;
            case CONTAINS:
                switch(predicate.attributeType()) {
                    case STRING:
                        return attributeValue.contains(predicate.valueString());
                    case INT:
                        return false;
                    case FLOAT:
                        return false;
                }
                break;
            case LESS_THAN:
                switch(predicate.attributeType()) {
                    case STRING:
                        return attributeValue.compareTo(predicate.valueString()) < 0;
                    case INT:
                        return Integer.valueOf(attributeValue) < predicate.valueInt();
                    case FLOAT:
                        return Double.valueOf(attributeValue) < predicate.valueFloat();
                }
                break;
            case GREATER_THAN:
                switch(predicate.attributeType()) {
                    case STRING:
                        return attributeValue.compareTo(predicate.valueString()) > 0;
                    case INT:
                        return Integer.valueOf(attributeValue) > predicate.valueInt();
                    case FLOAT:
                        return Double.valueOf(attributeValue) > predicate.valueFloat();
                }
                break;
            case LESS_EQUAL_THAN:
                switch(predicate.attributeType()) {
                    case STRING:
                        return attributeValue.compareTo(predicate.valueString()) <= 0;
                    case INT:
                        return Integer.valueOf(attributeValue) <= predicate.valueInt();
                    case FLOAT:
                        return Double.valueOf(attributeValue) <= predicate.valueFloat();
                }
                break;
            case GREATER_EQUAL_THAN:
                switch(predicate.attributeType()) {
                    case STRING:
                        return attributeValue.compareTo(predicate.valueString()) >= 0;
                    case INT:
                        return Integer.valueOf(attributeValue) >= predicate.valueInt();
                    case FLOAT:
                        return Double.valueOf(attributeValue) >= predicate.valueFloat();
                }
                break;
            case BEGIN_WITH:
                switch(predicate.attributeType()) {
                    case STRING:
                        return attributeValue.indexOf(predicate.valueString()) == 0;
                    case INT:
                        return false;
                    case FLOAT:
                        return false;
                }
            default:
                return false;
        }
        return false;
    }

    /**
     * Get the record Id of a record
     *
     * @param record
     * @return
     */
    public static String getRecordId(List<String> record) {

        return getAttributeValue(record,"id");
    }

    public static void main(String[] args) throws Exception {

        String host = "localhost";
        int port = 9999;

        if (args.length == 2) {
            host = args[0];
            port = Integer.parseInt(args[1]);
        }

        // (0) Load Event Rules
        loadEventRules();

        // - DEBUG - //
//        System.out.println("---------streamRecordRuleMap----------");
//        System.out.println(streamRecordRuleMap);
//        log.debug("---------streamRecordRuleMap----------");
//        log.debug(streamRecordRuleMap);
        // - DEBUG - //

        // (1) New a Stream Receiver
        SparkConf sparkConf = new SparkConf().setAppName("JavaCustomReceiver");
        JavaStreamingContext ssc = new JavaStreamingContext(sparkConf, new Duration(5000));
        // Create an input stream with the custom receiver on target ip:streamFromPort
        JavaReceiverInputDStream<String> lines = ssc.receiverStream(new SparkAppJava(host, port));

        // - DEBUG - //
//        System.out.println("---------Original Input----------");
//        lines.print(50);
//        log.info("---------Original Input----------");
//        log.info(lines);
        // - DEBUG - //


        // (2) Map the whole stream to different data source streams
        JavaPairDStream<String, List<String>> streams = lines.mapToPair(
                line -> {
                    String[] entries = line.split("\\|");
                    return new Tuple2<>(entries[0], Arrays.asList(entries));
                });

        // - DEBUG - //
//        System.out.println("---------Streams Separated----------");
//        streams.print(10);
//        log.debug("---------Streams Separated----------");
//        log.debug(streams);
        // - DEBUG - //


        // (3) Apply the predicates to each stream
        // [test-Thermometer_LT_80, ([Thermometer80Rule_r1], [ThermometerObservation, rid1, 80, 2018-11-08 00:00:00, sensor_id_x])]
        // [test-Thermometer_LT_80, ([Thermometer80Rule_r1], [ThermometerObservation, rid2, 92, 2018-11-08 00:00:00, sensor_id_y])]
        // [test-Thermometer_LT_80, ([Thermometer80Rule_r1], [ThermometerObservation, rid3, 131, 2018-11-08 00:00:00, sensor_id_z])]
        JavaDStream<Tuple2<List<IRecordRule>, List<List<String>>>> predicateMatchRecordList =
                streams.flatMap((FlatMapFunction<Tuple2<String, List<String>>, Tuple2<List<IRecordRule>, List<List<String>>>>) stream -> {
                    String streamName = stream._1();
                    // - DEBUG - //
//                    log.info("========== Process Stream: " + streamName + " ......");
//                    System.out.println("========== Process Stream: " + streamName + " ......");
                    // - DEBUG - //
                    if (streamRecordRuleMap.containsKey(streamName)) {

                        // - DEBUG - //
//                        log.debug("========== Stream: " + streamName + " matches to predicates.");
//                        System.out.println("========== Stream: " + streamName + " matches to predicates.");
                        // - DEBUG - //

                        ArrayList<Tuple2<List<IRecordRule>, List<List<String>>>> matchRecords = new ArrayList<>();

                        List<IRecordRule> recordRules = streamRecordRuleMap.get(streamName);
                        for (Iterator<IRecordRule> iter = recordRules.iterator(); iter.hasNext();) {

                            IRecordRule recordRule = iter.next();

                            // - DEBUG - //
//                            log.info("========== Rule: " + recordRule.getParent().ruleId());
//                            log.info("========== RecordRule: " + recordRule.id());
//                            System.out.println("========== Rule: " + recordRule.getParent().ruleId());
//                            System.out.println("========== RecordRule: " + recordRule.id());
                            // - DEBUG - //

                            //String attribute = recordRule.attribute();
                            //int indexOfAttribute = streamMetaMap.get(streamName).get(attribute);

                            // - DEBUG - //
//                            log.debug("========== indexOfAttribute: " + indexOfAttribute);
//                            System.out.println("========== indexOfAttribute: " + indexOfAttribute);
                            // - DEBUG - //

                            if (applyRecordRule(stream._2(), recordRule)) {
                                // -- No more needed -- //
//                                String recordId = getRecordId(stream._2());
//                                recordRule.setRecordId(recordId);
                                // -- No more needed -- //
                                List<IRecordRule> recordRuleList = new ArrayList<>();
                                List<List<String>> recordList = new ArrayList<>();
                                recordRuleList.add(recordRule);
                                recordList.add(stream._2());
                                matchRecords.add(new Tuple2<>(recordRuleList, recordList));
                            }
                        }
                        if (!matchRecords.isEmpty()) {
                            return matchRecords.iterator();
                        }
                        else {
                            return new ArrayList<Tuple2<List<IRecordRule>, List<List<String>>>>().iterator();
                        }
                    }
                    return new ArrayList<Tuple2<List<IRecordRule>, List<List<String>>>>().iterator();
                }).filter(record -> record != null);

        // - DEBUG - //
//        System.out.println("---------RecordRule Match Record List----------");
//        predicateMatchRecordList.print(50);
//        log.info("---------RecordRule Match Record List----------");
//        log.info(predicateMatchRecordList);
        // - DEBUG - //

        JavaPairDStream<String, Tuple2<List<IRecordRule>, List<List<String>>>> predicateMatchRecords =
                predicateMatchRecordList.mapToPair(matchRecord -> new Tuple2<>(matchRecord._1().get(0).getParent().ruleId(), matchRecord));

        // - DEBUG - //
//        System.out.println("---------RecordRule Match Records----------");
//        predicateMatchRecords.print(10);
//        log.debug("---------RecordRule Match Records----------");
//        log.debug(predicateMatchRecords);
        // - DEBUG - //


        // (4) Reduce the predicates of each Rule
        // [test-Thermometer_LT_80, ([Thermometer80Rule_p1, [[ThermometerObservation, rid1, 80, 2018-11-08 00:00:00, sensor_id_x],)]
        //                            Thermometer80Rule_p1,  [ThermometerObservation, rid2, 92, 2018-11-08 00:00:00, sensor_id_y],
        //                            Thermometer80Rule_p1]  [ThermometerObservation, rid3, 131, 2018-11-08 00:00:00, sensor_id_z]]
        JavaPairDStream<String, Tuple2<List<IRecordRule>, List<List<String>>>> reducedPredicateMatchRecords = predicateMatchRecords.reduceByKey(
                (Function2<Tuple2<List<IRecordRule>, List<List<String>>>, Tuple2<List<IRecordRule>, List<List<String>>>, Tuple2<List<IRecordRule>, List<List<String>>>>) (iRulePredicateListTuple2, iRulePredicateListTuple22) -> {

                    List<IRecordRule> predicateList1 = iRulePredicateListTuple2._1();
                    List<IRecordRule> predicateList2 = iRulePredicateListTuple22._1();
                    predicateList1.addAll(predicateList2);

                    List<List<String>> recordList1 = iRulePredicateListTuple2._2();
                    List<List<String>> recordList2 = iRulePredicateListTuple22._2();
                    recordList1.addAll(recordList2);

                    return new Tuple2<>(predicateList1, recordList1);
                });

        // - DEBUG - //
//        System.out.println("---------Reduced RecordRule Match Records----------");
//        reducedPredicateMatchRecords.print(50);
//        log.info("---------Reduced RecordRule Match Records----------");
//        log.info(reducedPredicateMatchRecords);
        // - DEBUG - //


        // (5) Merge the predicates for each Rule
        JavaPairDStream<String, List<List<String>>> ruleMatchRecords = reducedPredicateMatchRecords.mapToPair(
                stringTuple2Tuple2 -> {
                    String ruleId = stringTuple2Tuple2._1();
                    List<IRecordRule> recordRuleList = stringTuple2Tuple2._2()._1();
                    List<List<String>> recordList = stringTuple2Tuple2._2()._2();
                    IEventRule rule = recordRuleList.get(0).getParent();
                    if (rule.merger() != null) {
                        if (rule.merger().merge(recordRuleList)) {
                            return new Tuple2<>(ruleId, recordList);
                        }
                    }
                    return null;
            }).filter(record -> record != null);

        // ADD SOMETHING HERE TO INFORM GOOGLE THAT WE HAVE ONE OR BUNCH OF RECORDS
        //createAndPublish.createTopic();
        //createAndPublish.publishMessages(output.toString());

        // - DEBUG - //
        System.out.println("---------Rule Match Records----------");
        ruleMatchRecords.print(100);
        log.info("---------Rule Match Records----------");
        log.info(ruleMatchRecords);
        // - DEBUG - //

        ssc.start();
        ssc.awaitTermination();
    }

    public SparkAppJava(String host_ , int port_) {
        super(StorageLevel.MEMORY_AND_DISK_2());
        streamFromHost = host_;
        streamFromPort = port_;

        Map<String, Integer> ThermometerObservation = new HashMap<>();
        ThermometerObservation.put("id", 1);
        ThermometerObservation.put("temperature", 2);
        ThermometerObservation.put("timeStamp", 3);
        ThermometerObservation.put("sensor_id", 4);
        streamMetaMap.put("ThermometerObservation", ThermometerObservation);

        Map<String, Integer> WiFiAPObservation = new HashMap<>();
        WiFiAPObservation.put("id", 1);
        WiFiAPObservation.put("clientId", 2);
        WiFiAPObservation.put("timeStamp", 3);
        WiFiAPObservation.put("sensor_id", 4);
        streamMetaMap.put("WiFiAPObservation", WiFiAPObservation);

        Map<String, Integer> WeMoObservation = new HashMap<>();
        WeMoObservation.put("id", 1);
        WeMoObservation.put("currentMilliWatts", 2);
        WeMoObservation.put("onTodaySeconds", 3);
        WeMoObservation.put("timeStamp", 4);
        WeMoObservation.put("sensor_id", 5);
        streamMetaMap.put("WeMoObservation", WeMoObservation);

        Map<String, Integer> PRESENCE = new HashMap<>();
        PRESENCE.put("id", 1);
        PRESENCE.put("semantic_entity_id", 2);
        PRESENCE.put("location", 3);
        PRESENCE.put("timeStamp", 4);
        PRESENCE.put("virtual_sensor_id", 5);
        streamMetaMap.put("PRESENCE", PRESENCE);

        Map<String, Integer> OCCUPANCY = new HashMap<>();
        OCCUPANCY.put("id", 1);
        OCCUPANCY.put("semantic_entity_id", 2);
        OCCUPANCY.put("occupancy", 3);
        OCCUPANCY.put("timeStamp", 4);
        OCCUPANCY.put("virtual_sensor_id", 5);
        streamMetaMap.put("OCCUPANCY", OCCUPANCY);
    }

    @Override
    public void onStart() {
        // Start the thread that receives data over a connection
        new Thread(this::receive).start();
    }

    @Override
    public void onStop() {
        // There is nothing much to do as the thread calling receive()
        // is designed to stop by itself isStopped() returns false
    }

    /** Create a socket connection and receive data until receiver is stopped */
    private void receive() {
        try {
            Socket socket = null;
            BufferedReader reader = null;
            try {
                // connect to the server
                socket = new Socket(streamFromHost, streamFromPort);
                reader = new BufferedReader(
                        new InputStreamReader(socket.getInputStream(), StandardCharsets.UTF_8));
                // Until stopped or connection broken continue reading
                String userInput;
                while (!isStopped() && (userInput = reader.readLine()) != null) {
                    //System.out.println("Received data '" + userInput + "'");
                    //log.debug("---------Original Input-----------");
                    //log.debug(userInput);
                    store(userInput);
                }
            } finally {
                Closeables.close(reader, /* swallowIOException = */ true);
                Closeables.close(socket,  /* swallowIOException = */ true);
            }
            // Restart in an attempt to connect again when server is active again
            restart("Trying to connect again");
        } catch(ConnectException ce) {
            // restart if could not connect to server
            restart("Could not connect", ce);
        } catch(Throwable t) {
            restart("Error receiving data", t);
        }
    }
}
