package cs237;

import org.apache.log4j.LogManager;

import java.io.*;
import java.net.Authenticator;
import java.net.HttpURLConnection;
import java.net.PasswordAuthentication;
import java.net.URL;
import java.util.List;

public class Publisher {

  public static org.apache.log4j.Logger log = LogManager.getRootLogger();

  public static boolean sendMessageByHttp(String topic, String message) {

    boolean successful = false;

    try {

      Authenticator.setDefault(new Authenticator() {

        @Override
        protected PasswordAuthentication getPasswordAuthentication() {
          return new PasswordAuthentication("admin", "admin".toCharArray());
        }
      });

      // - DEBUG - //
      log.error("========== Publisher is publishing message: [" + message + "] to topic: [" + topic + "].");
      // - DEBUG - //

      URL url = new URL ("http://localhost:8161/api/message?destination=topic://" + topic);
      HttpURLConnection con = (HttpURLConnection) url.openConnection();
      con.setRequestMethod("POST");
      con.setRequestProperty("User-Agent", "Mozilla/5.0");

      // For POST only - START
      con.setDoOutput(true);
      OutputStream os = con.getOutputStream();
      String body = "body=" + message;
      os.write(body.getBytes());
      os.flush();
      os.close();
      // For POST only - END

      // Get POST response
      int responseCode = con.getResponseCode();
      log.error("========== POST Response Code :: " + responseCode);

      if (responseCode == HttpURLConnection.HTTP_OK) { //success
        BufferedReader in = new BufferedReader(new InputStreamReader(
                con.getInputStream()));
        String inputLine;
        StringBuffer response = new StringBuffer();

        while ((inputLine = in.readLine()) != null) {
          response.append(inputLine);
        }
        in.close();

        log.error("========== [Publisher] POST Succeed!!!");
        log.error("========== POST Response :: " + response.toString());
        successful = true;
      } else {
        log.error("========== [Publisher] POST Failed!!!");
        successful = false;
      }

    } catch(Exception e) {
      log.error("========== [Publisher] POST Exception" + e.getMessage());
      successful = false;
    }

    return successful;
  }

  public static void main(String[] args) {

    try {

      Publisher.sendMessageByHttp("test-UserInBuildingAndRoomEmpty", "Yeah!!");

    }
    catch (Exception e) {
      e.printStackTrace();
    }
  }

  public static int sendMessagesByHttp(String topic, List<String> messages) {

    int successfulCount = 0;

    for (int i = 0; i < messages.size(); i ++) {
      String message = messages.get(i);
      if(sendMessageByHttp(topic, message))
        successfulCount ++;
    }

    return successfulCount;
  }

}
