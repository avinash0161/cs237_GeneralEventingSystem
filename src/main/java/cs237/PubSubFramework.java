package cs237;

import com.google.api.core.ApiFuture;
import com.google.api.core.ApiFutures;
import com.google.api.gax.rpc.ApiException;
import com.google.cloud.ServiceOptions;
import com.google.cloud.pubsub.v1.Publisher;
import com.google.cloud.pubsub.v1.TopicAdminClient;
import com.google.protobuf.ByteString;
import com.google.pubsub.v1.ProjectTopicName;
import com.google.pubsub.v1.PubsubMessage;

import java.util.ArrayList;
import java.util.List;

public class PubSubFramework {

    // Your Google Cloud Platform project ID
    public static String projectId = ServiceOptions.getDefaultProjectId();

    // Your topic ID, eg. "my-topic"
    //public static String topicId = "TEST123";

    // Your subscriber ID
    public static String subsId = "USER_TEST";

    private static void createTopic(String topicId) throws Exception {

        // Create a new topic
        ProjectTopicName topic = ProjectTopicName.of(projectId, topicId);
        try (TopicAdminClient topicAdminClient = TopicAdminClient.create()) {
            topicAdminClient.createTopic(topic);
        } catch (ApiException e) {
            // example : code = ALREADY_EXISTS(409) implies topic already exists
            System.out.print(e.getStatusCode().getCode());
            System.out.print(e.isRetryable());
        }
        System.out.printf("Topic %s:%s created.\n", topic.getProject(), topic.getTopic());
    }

    public static void publishMessages(String topicId, List<String> messages) throws Exception {

        createTopic(topicId);

        // [START pubsub_publish]
        ProjectTopicName topicName = ProjectTopicName.of(projectId, topicId);
        Publisher publisher = null;
        List<ApiFuture<String>> messageIdFutures = new ArrayList<>();

        try {
            // Create a publisher instance with default settings bound to the topic
            publisher = Publisher.newBuilder(topicName).build();

            // schedule publishing one message at a time : messages get automatically batched
            for (String message : messages) {
                ByteString data = ByteString.copyFromUtf8(message);
                PubsubMessage pubsubMessage = PubsubMessage.newBuilder().setData(data).build();

                // Once published, returns a server-assigned message id (unique within the topic)
                ApiFuture<String> messageIdFuture = publisher.publish(pubsubMessage);
                messageIdFutures.add(messageIdFuture);
            }
        } finally {
            // wait on any pending publish requests.
            List<String> messageIds = ApiFutures.allAsList(messageIdFutures).get();

            for (String messageId : messageIds) {
                System.out.println("published with message ID: " + messageId);
            }

            if (publisher != null) {
                // When finished with the publisher, shutdown to free up resources.
                publisher.shutdown();
            }
        }
        // [END pubsub_publish]
    }

/*    public static void publishMessagesWithErrorHandler() throws Exception {
        // [START pubsub_publish_error_handler]
        ProjectTopicName topicName = ProjectTopicName.of("my-project-id", "my-topic-id");
        Publisher publisher = null;

        try {
            // Create a publisher instance with default settings bound to the topic
            publisher = Publisher.newBuilder(topicName).build();

            List<String> messages = Arrays.asList("first message", "second message");

            for (final String message : messages) {
                ByteString data = ByteString.copyFromUtf8(message);
                PubsubMessage pubsubMessage = PubsubMessage.newBuilder().setData(data).build();

                // Once published, returns a server-assigned message id (unique within the topic)
                ApiFuture<String> future = publisher.publish(pubsubMessage);

                // Add an asynchronous callback to handle success / failure
                ApiFutures.addCallback(future, new ApiFutureCallback<String>() {

                    @Override
                    public void onFailure(Throwable throwable) {
                        if (throwable instanceof ApiException) {
                            ApiException apiException = ((ApiException) throwable);
                            // details on the API exception
                            System.out.println(apiException.getStatusCode().getCode());
                            System.out.println(apiException.isRetryable());
                        }
                        System.out.println("Error publishing message : " + message);
                    }

                    @Override
                    public void onSuccess(String messageId) {
                        // Once published, returns server-assigned message ids (unique within the topic)
                        System.out.println(messageId);
                    }
                });
            }
        } finally {
            if (publisher != null) {
                // When finished with the publisher, shutdown to free up resources.
                publisher.shutdown();
            }
        }
        // [END pubsub_publish_error_handler]
    }*/

    /*
    public static void createSubscriber() throws Exception {
        ProjectTopicName topic = ProjectTopicName.of(projectId, topicId);
        ProjectSubscriptionName subscription = ProjectSubscriptionName.of(projectId, subsId);

        try (SubscriptionAdminClient subscriptionAdminClient = SubscriptionAdminClient.create()) {
            subscriptionAdminClient.createSubscription(subscription, topic, PushConfig.getDefaultInstance(), 10);
        }

        MessageReceiver receiver =
                new MessageReceiver() {
                    @Override
                    public void receiveMessage(PubsubMessage message, AckReplyConsumer consumer) {
                        System.out.println("Received message: " + message.getData().toStringUtf8());
                        consumer.ack();
                    }
                };
        Subscriber subscriber = null;
        try {
            subscriber = Subscriber.newBuilder(subscription, receiver).build();
            subscriber.addListener(
                    new Subscriber.Listener() {
                        @Override
                        public void failed(Subscriber.State from, Throwable failure) {
                            // Handle failure. This is called when the Subscriber encountered a fatal error and is shutting down.
                            System.err.println(failure);
                        }
                    },
                    MoreExecutors.directExecutor());
            subscriber.startAsync().awaitRunning();

            // In this example, we will pull messages for one minute (60,000ms) then stop.
            // In a real application, this sleep-then-stop is not necessary.
            // Simply call stopAsync().awaitTerminated() when the server is shutting down, etc.
            Thread.sleep(60000);
        } finally {
            if (subscriber != null) {
                subscriber.stopAsync().awaitTerminated();
            }
        }
    }
    */

    public static void main(String... args) throws Exception {
        //createTopic();
        //publishMessages();
    }
}
