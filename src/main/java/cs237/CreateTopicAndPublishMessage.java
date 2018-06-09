/*
 * Copyright 2016 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package cs237;

import com.google.api.core.ApiFuture;
import com.google.api.core.ApiFutureCallback;
import com.google.api.core.ApiFutures;
import com.google.api.gax.rpc.ApiException;
import com.google.cloud.ServiceOptions;
import com.google.cloud.pubsub.v1.Publisher;
import com.google.cloud.pubsub.v1.TopicAdminClient;
import com.google.protobuf.ByteString;
import com.google.pubsub.v1.ProjectTopicName;
import com.google.pubsub.v1.PubsubMessage;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * A snippet for Google Cloud Pub/Sub showing how to create a Pub/Sub topic and asynchronously
 * publish messages to it.
 */
public class CreateTopicAndPublishMessage {

    // Your Google Cloud Platform project ID
    public static String projectId = ServiceOptions.getDefaultProjectId();

    // Your topic ID, eg. "my-topic"
    public static String topicId = "ThermometerObservationGT80";

    public static void createTopic() throws Exception {
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

    public static void publishMessages(String args) throws Exception {
        // [START pubsub_publish]
        ProjectTopicName topicName = ProjectTopicName.of(projectId, topicId);
        Publisher publisher = null;
        List<ApiFuture<String>> messageIdFutures = new ArrayList<>();

        try {
            // Create a publisher instance with default settings bound to the topic
            publisher = Publisher.newBuilder(topicName).build();

            // List<String> messages = Arrays.asList("first message", "second message");
            List<String> messages = new ArrayList<String>();
            //for (int i = 0; i < args.length; i++) {
            //    messages.add(args[i]);
            //}

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

    public static void publishMessagesWithErrorHandler() throws Exception {
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
    }

    /*
    public static void main(String... args) throws Exception {
        String[] outputStr = new String[] {"(a0591d80_b6ee_4db4_a7e5_913cf0f2003e-2017-11-08 00:30:00,34)",
                "(48ec9043_4d33_4fc5_b79d_62eca3864f74-2017-11-08 00:30:00,31)",
                "(eb5c404c_3456_4041_841f_16347a493d36-2017-11-08 00:30:00,10)",
                "(cf4e7edc_33fd_4112_b8dd_d1d098712eaf-2017-11-08 00:30:00,44)",
                "(29a9e39e_f73b_441c_9709_71b9f90b54df-2017-11-08 00:30:00,42)",
                "(8d8cc3cb_87cd_49b5_9770_1786f8fd8170-2017-11-08 00:30:00,44)",
                "(da10f8a6_1382_460a_943b_8c2383f67779-2017-11-08 00:30:00,60)",
                "(6ed36721_63d2_48b3_af93_0066ffb20308-2017-11-08 00:30:00,84)",
                "(afa49397_9b8b_4468_8d26_189e95ae819a-2017-11-08 00:30:00,98)",
                "(5ba0167e_a57f_445f_b734_6f6c72231bcc-2017-11-08 00:30:00,56)"};
        createTopic();
        publishMessages(outputStr);
    }
    */
}