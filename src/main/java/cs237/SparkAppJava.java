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
    public static Map<String, IRuleMerger> ruleMergerMap = new HashMap<>();
    public static Map<String, List<IRulePredicate>> streamPredicateMap = new HashMap<>();

    public static Logger log = LogManager.getRootLogger();

    /**
     * Load Event Rules
     * TODO - (1) Load the rules list from some file or DB. (2) Also load the java objects from jar files.
     */
    public static void loadEventRules() {

        // (1) Load the Rules from somewhere.
        List<IEventRule> eventRuleList = new ArrayList<>();
        IEventRule rule0 = new Thermometer80LTRule();
        eventRuleList.add(rule0);
        ruleMergerMap.put(rule0.ruleId(), rule0.merger());

        // (2) Construct the Map of stream to List of Predicates;
        eventRuleList.forEach(eventRule -> {
            eventRule.predicateList().forEach(predicate -> {
                if (streamPredicateMap.containsKey(predicate.stream())) {
                     streamPredicateMap.get(predicate.stream()).add(predicate);
                }
                else {
                    streamPredicateMap.put(predicate.stream(), Arrays.asList(predicate));
                }
            });
        });
    }

    /**
     * Apply a predicate on an attribute
     *
     * @param attribute
     * @param predicate
     * @return
     */
    public static boolean applyPredicate(String attribute, IRulePredicate predicate) {

        switch(predicate.operator()) {
            case EQUAL:
                switch(predicate.attributeType()) {
                    case STRING:
                        return attribute.equals(predicate.valueString());
                    case INT:
                        return Integer.valueOf(attribute) == predicate.valueInt();
                    case FLOAT:
                        return Double.valueOf(attribute) == predicate.valueFloat();
                }
                break;
            case CONTAINS:
                switch(predicate.attributeType()) {
                    case STRING:
                        return attribute.contains(predicate.valueString());
                    case INT:
                        return false;
                    case FLOAT:
                        return false;
                }
                break;
            case LESS_THAN:
                switch(predicate.attributeType()) {
                    case STRING:
                        return attribute.compareTo(predicate.valueString()) < 0;
                    case INT:
                        return Integer.valueOf(attribute) < predicate.valueInt();
                    case FLOAT:
                        return Double.valueOf(attribute) < predicate.valueFloat();
                }
                break;
            case LARGER_THAN:
                switch(predicate.attributeType()) {
                    case STRING:
                        return attribute.compareTo(predicate.valueString()) > 0;
                    case INT:
                        return Integer.valueOf(attribute) > predicate.valueInt();
                    case FLOAT:
                        return Double.valueOf(attribute) > predicate.valueFloat();
                }
                break;
            case LESS_EQUAL_THAN:
                switch(predicate.attributeType()) {
                    case STRING:
                        return attribute.compareTo(predicate.valueString()) <= 0;
                    case INT:
                        return Integer.valueOf(attribute) <= predicate.valueInt();
                    case FLOAT:
                        return Double.valueOf(attribute) <= predicate.valueFloat();
                }
                break;
            case LARGER_EQUAL_THAN:
                switch(predicate.attributeType()) {
                    case STRING:
                        return attribute.compareTo(predicate.valueString()) >= 0;
                    case INT:
                        return Integer.valueOf(attribute) >= predicate.valueInt();
                    case FLOAT:
                        return Double.valueOf(attribute) >= predicate.valueFloat();
                }
                break;
            default:
                return false;
        }
        return false;
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
//        System.out.println("---------streamPredicateMap----------");
//        System.out.println(streamPredicateMap);
//        log.debug("---------streamPredicateMap----------");
//        log.debug(streamPredicateMap);
        // - DEBUG - //

        // (1) New a Stream Receiver
        SparkConf sparkConf = new SparkConf().setAppName("JavaCustomReceiver");
        JavaStreamingContext ssc = new JavaStreamingContext(sparkConf, new Duration(5000));
        // Create an input stream with the custom receiver on target ip:streamFromPort
        JavaReceiverInputDStream<String> lines = ssc.receiverStream(new SparkAppJava(host, port));

        // - DEBUG - //
//        System.out.println("---------Original Input----------");
//        lines.print(10);
//        log.debug("---------Original Input----------");
//        log.debug(lines);
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
        // [test-Thermometer_LT_80, ([Thermometer80Rule_p1], [ThermometerObservation, rid1, 80, 2018-11-08 00:00:00, sensor_id_x])]
        // [test-Thermometer_LT_80, ([Thermometer80Rule_p1], [ThermometerObservation, rid2, 92, 2018-11-08 00:00:00, sensor_id_y])]
        // [test-Thermometer_LT_80, ([Thermometer80Rule_p1], [ThermometerObservation, rid3, 131, 2018-11-08 00:00:00, sensor_id_z])]
        JavaDStream<Tuple2<List<IRulePredicate>, List<List<String>>>> predicateMatchRecordList =
                streams.flatMap((FlatMapFunction<Tuple2<String, List<String>>, Tuple2<List<IRulePredicate>, List<List<String>>>>) stream -> {
                    String streamName = stream._1();
                    // - DEBUG - //
//                    log.debug("========== Process Stream: " + streamName + " ......");
//                    System.out.println("========== Process Stream: " + streamName + " ......");
                    // - DEBUG - //
                    if (streamPredicateMap.containsKey(streamName)) {

                        // - DEBUG - //
//                        log.debug("========== Stream: " + streamName + " matches to predicates.");
//                        System.out.println("========== Stream: " + streamName + " matches to predicates.");
                        // - DEBUG - //

                        ArrayList<Tuple2<List<IRulePredicate>, List<List<String>>>> matchRecords = new ArrayList<>();

                        List<IRulePredicate> predicates = streamPredicateMap.get(streamName);
                        for (Iterator<IRulePredicate> iter = predicates.iterator(); iter.hasNext();) {

                            IRulePredicate predicate = iter.next();

                            // - DEBUG - //
//                            log.debug("========== Rule: " + predicate.getParent().ruleId());
//                            log.debug("========== Predicate: " + predicate.id());
//                            log.debug("==========     Attribute: " + predicate.attribute());
//                            log.debug("==========     AttributeType: " + predicate.attributeType());
//                            log.debug("==========     Operator: " + predicate.operator());
//                            log.debug("==========     ValueString: " + predicate.valueString());
//                            log.debug("==========     ValueInt: " + predicate.valueInt());
//                            log.debug("==========     ValueString: " + predicate.valueFloat());
//                            System.out.println("========== Rule: " + predicate.getParent().ruleId());
//                            System.out.println("========== Predicate: " + predicate.id());
//                            System.out.println("==========     Attribute: " + predicate.attribute());
//                            System.out.println("==========     AttributeType: " + predicate.attributeType());
//                            System.out.println("==========     Operator: " + predicate.operator());
//                            System.out.println("==========     ValueString: " + predicate.valueString());
//                            System.out.println("==========     ValueInt: " + predicate.valueInt());
//                            System.out.println("==========     ValueString: " + predicate.valueFloat());
                            // - DEBUG - //

                            String attribute = predicate.attribute();
                            int indexOfAttribute = streamMetaMap.get(streamName).get(attribute);

                            // - DEBUG - //
//                            log.debug("========== indexOfAttribute: " + indexOfAttribute);
//                            System.out.println("========== indexOfAttribute: " + indexOfAttribute);
                            // - DEBUG - //

                            if (applyPredicate(stream._2().get(indexOfAttribute), predicate)) {
                                List<IRulePredicate> predicateList = new ArrayList<>();
                                List<List<String>> recordList = new ArrayList<>();
                                predicateList.add(predicate);
                                recordList.add(stream._2());
                                matchRecords.add(new Tuple2<>(predicateList, recordList));
                            }
                        }
                        if (!matchRecords.isEmpty()) {
                            return matchRecords.iterator();
                        }
                        else {
                            return new ArrayList<Tuple2<List<IRulePredicate>, List<List<String>>>>().iterator();
                        }
                    }
                    return new ArrayList<Tuple2<List<IRulePredicate>, List<List<String>>>>().iterator();
                }).filter(record -> record != null);

        // - DEBUG - //
//        System.out.println("---------Predicate Match Record List----------");
//        predicateMatchRecordList.print(10);
//        log.debug("---------Predicate Match Record List----------");
//        log.debug(predicateMatchRecordList);
        // - DEBUG - //

        JavaPairDStream<String, Tuple2<List<IRulePredicate>, List<List<String>>>> predicateMatchRecords =
                predicateMatchRecordList.mapToPair(matchRecord -> new Tuple2<>(matchRecord._1().get(0).getParent().ruleId(), matchRecord));

        // - DEBUG - //
//        System.out.println("---------Predicate Match Records----------");
//        predicateMatchRecords.print(10);
//        log.debug("---------Predicate Match Records----------");
//        log.debug(predicateMatchRecords);
        // - DEBUG - //


        // (4) Reduce the predicates of each Rule
        // [test-Thermometer_LT_80, ([Thermometer80Rule_p1, [[ThermometerObservation, rid1, 80, 2018-11-08 00:00:00, sensor_id_x],)]
        //                            Thermometer80Rule_p1,  [ThermometerObservation, rid2, 92, 2018-11-08 00:00:00, sensor_id_y],
        //                            Thermometer80Rule_p1]  [ThermometerObservation, rid3, 131, 2018-11-08 00:00:00, sensor_id_z]]
        JavaPairDStream<String, Tuple2<List<IRulePredicate>, List<List<String>>>> reducedPredicateMatchRecords = predicateMatchRecords.reduceByKey(
                (Function2<Tuple2<List<IRulePredicate>, List<List<String>>>, Tuple2<List<IRulePredicate>, List<List<String>>>, Tuple2<List<IRulePredicate>, List<List<String>>>>) (iRulePredicateListTuple2, iRulePredicateListTuple22) -> {

                    List<IRulePredicate> predicateList1 = iRulePredicateListTuple2._1();
                    List<IRulePredicate> predicateList2 = iRulePredicateListTuple22._1();
                    predicateList1.addAll(predicateList2);

                    List<List<String>> recordList1 = iRulePredicateListTuple2._2();
                    List<List<String>> recordList2 = iRulePredicateListTuple22._2();
                    recordList1.addAll(recordList2);

                    return new Tuple2<>(predicateList1, recordList1);
                });

        // - DEBUG - //
//        System.out.println("---------Reduced Predicate Match Records----------");
//        reducedPredicateMatchRecords.print(10);
//        log.debug("---------Reduced Predicate Match Records----------");
//        log.debug(reducedPredicateMatchRecords);
        // - DEBUG - //


        // (5) Merge the predicates for each Rule
        JavaPairDStream<String, List<List<String>>> ruleMatchRecords = reducedPredicateMatchRecords.mapToPair(
                stringTuple2Tuple2 -> {
                    String ruleId = stringTuple2Tuple2._1();
                    List<IRulePredicate> predicateList = stringTuple2Tuple2._2()._1();
                    List<List<String>> recordList = stringTuple2Tuple2._2()._2();
                    IEventRule rule = predicateList.get(0).getParent();
                    if (rule.merger() != null) {
                        if (rule.merger().merge(predicateList)) {
                            return new Tuple2<>(ruleId, recordList);
                        }
                    }
                    return null;
            }).filter(record -> record != null);

        // - DEBUG - //
        System.out.println("---------Rule Match Records----------");
        ruleMatchRecords.print(10);
        log.debug("---------Rule Match Records----------");
        log.debug(ruleMatchRecords);
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
