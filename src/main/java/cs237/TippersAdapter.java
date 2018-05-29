package cs237;

import java.io.IOException;
import java.io.PrintWriter;
import java.sql.*;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;
import java.net.ServerSocket;
import java.net.Socket;

public class TippersAdapter {

    DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

    private Connection sourceDBConnection = null;

    // executor service is the executor (worker) thread pool
    // where the master thread can submit jobs to the executors
    private ThreadPoolExecutor dataPushingService = null;

    // how many data pullers (threads) we want to have for data pulling
    // currently we have 3 types of sensors observations and 2 types of semantic observations
    public static int numOfDataPullers = 5;

    // Pulling data from this startDate
    public static LocalDateTime startDate = LocalDateTime.of(2017,11, 8, 0, 0, 0);

    // Pulling data frequency
    // Meaning: For every how many seconds will this Puller clock tick.
    public static int tickFrequency = 5;

    // Pulling data frequency scale
    // Meaning: How many times of time of data in real world will be pulled every 1 tick of this simulation clock.
    public static int frequencyScale = 12;

    // The puller thread which will be created and run once a client connects to this Socket Server.
    private static class Puller extends Thread {
        private Socket socket;

        public Puller(Socket socket) {
            this. socket = socket;
        }

        public void run() {
            try {

                TippersAdapter tippersAdapter = new TippersAdapter();
                tippersAdapter.setupExecutors();
                tippersAdapter.connectSourceDataDB();

                List<String> tables = new ArrayList<>();
                tables.add("WeMoObservation");
                tables.add("WiFiAPObservation");
                tables.add("ThermometerObservation");
                tables.add("OCCUPANCY");
                tables.add("PRESENCE");

                tippersAdapter.runPulling(tables, socket);

                tippersAdapter.closeSourceDBConnection();

            } catch (IOException e) {
                e.printStackTrace();
            } catch (Exception e) {
                e.printStackTrace();
            } finally {
                try {
                    socket.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    public static void main(String[] args) throws Exception {

        ServerSocket listener = new ServerSocket(9999);

        try {
            while (true) {
                new Puller(listener.accept()).start();
            }
        } finally {
            listener.close();
        }
    }

    private void connectSourceDataDB() throws Exception {
        Class.forName("org.sqlite.JDBC").newInstance();
        sourceDBConnection = DriverManager.getConnection("jdbc:sqlite:cs237.db");
    }

    private void closeSourceDBConnection() throws Exception {
        if (sourceDBConnection != null) {
            sourceDBConnection.close();
        }
    }

    private void setupExecutors() {
        // set up a fixed thread pool
        this.dataPushingService = (ThreadPoolExecutor) Executors.newFixedThreadPool(numOfDataPullers);
    }

    public void runPulling(List<String> tables) throws Exception {

        runPulling(tables, null);
    }

    public void runPulling(List<String> tables, Socket socket) throws Exception {

        LocalDateTime startTimeStamp;
        LocalDateTime endTimeStamp = startDate;

        while (true) {

            long startTime = System.currentTimeMillis();

            startTimeStamp = endTimeStamp;
            endTimeStamp = endTimeStamp.plusSeconds(tickFrequency * frequencyScale);

            System.out.println("Pulling data between [ "
                    + startTimeStamp.format(DateTimeFormatter.ISO_LOCAL_DATE_TIME)
                    + ", "
                    + endTimeStamp.format(DateTimeFormatter.ISO_LOCAL_DATE_TIME)
                    + " ]."
            );

            // Each table (observation type)
            for (String tableName : tables) {
                List<String> dataRecords = new ArrayList<>();

                dataRecords.addAll(this.getDataRecordsBetweenTimestamps(startTimeStamp, endTimeStamp, tableName));

                this.dataPushingService.submit(() -> this.push(dataRecords, socket));
            }

            long endTime = System.currentTimeMillis();
            long driftTime = endTime - startTime;

            if (driftTime < tickFrequency * 1000) {
                Thread.sleep(tickFrequency * 1000 - driftTime);
            } else {
                System.out.println("Pulling process is slower than real-time data generation by ["
                        + (driftTime - 1000)
                        + "] ms."
                );
            }

        }
    }

    private Void push(List<String> dataRecords, Socket socket) {
        if (dataRecords.isEmpty()) {
            System.out.println("dataRecords is empty");
            return null;
        }

        try {

            PrintWriter socketOut = new PrintWriter(socket.getOutputStream(), true);

            for (String record : dataRecords) {

                System.out.println("Pushing data: " + record);

                socketOut.println(record);
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {

        }

        return null;
    }

    private ArrayList<String> getDataRecordsBetweenTimestamps(LocalDateTime sTimestamp,
                                                              LocalDateTime eTimestamp,
                                                              String tableName) {

        try {
            ArrayList<String> dataRecords = new ArrayList<>();
            PreparedStatement preparedStatement;
            String queryTemplate = "select * from " + tableName + " where timestamp between ? and ?";
            preparedStatement = sourceDBConnection.prepareStatement(queryTemplate);
            preparedStatement.setString(1, sTimestamp.format(formatter));
            preparedStatement.setString(2, eTimestamp.format(formatter));

            ResultSet resultSet = preparedStatement.executeQuery();
            ResultSetMetaData metaData = resultSet.getMetaData();
            int numberOfColumns = metaData.getColumnCount();
            while (resultSet.next()) {
                StringBuilder record = new StringBuilder();
                record.append(tableName);
                record.append("|");
                for (int i = 1; i <= numberOfColumns; i ++) {
                    if (i > 1) {
                        record.append("|");
                    }
                    record.append(resultSet.getString(i));
                }
                dataRecords.add(record.toString());
            }
            resultSet.close();
            preparedStatement.close();

            return dataRecords;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

}
