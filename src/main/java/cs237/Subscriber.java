package cs237;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.Session;
import javax.jms.TextMessage;
import javax.jms.Topic;

import org.apache.activemq.ActiveMQConnection;
import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.log4j.LogManager;

public class Subscriber {

  public static org.apache.log4j.Logger log = LogManager.getRootLogger();

  private Connection connection;
  private MessageConsumer messageConsumer;

  public void create(String clientId, String topicName)
      throws JMSException {

    // create a Connection Factory
    ConnectionFactory connectionFactory =
        new ActiveMQConnectionFactory(
            ActiveMQConnection.DEFAULT_BROKER_URL);

    // create a Connection
    connection = connectionFactory.createConnection();
    connection.setClientID(clientId);

    // create a Session
    Session session =
        connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

    // create the Topic from which messages will be received
    Topic topic = session.createTopic(topicName);

    // create a MessageConsumer for receiving messages
    messageConsumer = session.createConsumer(topic);

    // start the connection in order to receive messages
    connection.start();
  }

  public void closeConnection() throws JMSException {
    connection.close();
  }

  public String getMessage(int timeout) throws JMSException {

    // read a message from the topic destination
    Message message = messageConsumer.receive(timeout);

    String text = null;
    // check if a message was received
    if (message != null) {
      // cast the message to the correct type
      TextMessage textMessage = (TextMessage) message;

      // retrieve the message content
      text = textMessage.getText();

    } else {
      //log.error(clientId + ": no message received");
    }

    return text;
  }

  public static void main(String[] args) {

    Subscriber subscriber1 = new Subscriber();

    Subscriber subscriber2 = new Subscriber();

    try {
      subscriber1.create("test-TwoUserPresentSameFloor", "test-TwoUserPresentSameFloor");
      subscriber2.create("test-UserInBuildingAndRoomEmpty", "test-UserInBuildingAndRoomEmpty");

      for(;;) {
        String msg1 = subscriber1.getMessage(1000);
        String msg2 = subscriber2.getMessage(1000);
        if (msg1 != null) {
          System.out.println("========== [Message Received] Topic : test-TwoUserPresentSameFloor --------> Message: " + msg1);
        }
        if (msg2 != null) {
          System.out.println("========== [Message Received] Topic : test-UserInBuildingAndRoomEmpty --------> Message: " + msg2);
        }

      }
    }
    catch (Exception e) {
      e.printStackTrace();
    }
  }
}
