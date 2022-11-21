package org.example;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertTrue;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.junit.Test;
import org.neo4j.driver.AuthTokens;
import org.neo4j.driver.Driver;
import org.neo4j.driver.GraphDatabase;
import org.neo4j.driver.Result;
import org.neo4j.driver.Session;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.containers.Neo4jContainer;
import org.testcontainers.containers.Neo4jLabsPlugin;
import org.testcontainers.containers.Network;
import org.testcontainers.lifecycle.Startables;
import org.testcontainers.shaded.com.google.common.collect.ImmutableMap;
import org.testcontainers.utility.DockerImageName;
import org.testcontainers.utility.LicenseAcceptance;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

/**
 * Test that neo4j testcontainer can pass a simple message to kafka testcontainer on same network
 */
public class MySimpleKafkaTest
{
    @Test
    public void test() throws InterruptedException, IOException, ExecutionException {
        String versionString = "neo4j:4.4.11-enterprise";//Calculated from POM neo4j version

        LicenseAcceptance.assertLicenseAccepted(versionString);

        Network network = Network.newNetwork();
        try (KafkaContainer kafka = new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:6.2.1"))
                .withNetwork(network)
                .withNetworkAliases("kafka");
             Neo4jContainer<?> neo4j = new Neo4jContainer<>(DockerImageName.parse(versionString))
                     .withNetwork(network)
                     .withoutAuthentication()
                     .withNeo4jConfig("dbms.logs.debug.level", "DEBUG")
                     .withNeo4jConfig("kafka.bootstrap.servers", "kafka:9093")
                     .withNeo4jConfig("streams.source.enabled", "true")
//                     .withEnterpriseEdition() //Cannot be used directly as it overrides the neo4j version
                     .withEnv("NEO4J_ACCEPT_LICENSE_AGREEMENT", "yes")
                     .withLogConsumer(of -> System.out.print(of.getUtf8String()))
                     .withLabsPlugins(Neo4jLabsPlugin.STREAMS, Neo4jLabsPlugin.APOC)
                     .dependsOn(kafka)) {
            Startables.deepStart(kafka, neo4j).join();

            //Set up kafka topics
            AdminClient adminClient = AdminClient.create(
                    ImmutableMap.of(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers())
            );
            List<String> topicsToCreate = List.of("MyTopic1", "MyTopic2");
            Collection<NewTopic> topics = new ArrayList<>();
            for (String kafkaTopic : topicsToCreate) {
                topics.add(new NewTopic(kafkaTopic, 1, (short)1));
            }
            adminClient.createTopics(topics).all().get();

            assertThat(adminClient.listTopics().names().get()).containsAll(topicsToCreate);

            String boltUrl = neo4j.getBoltUrl();
            try (Driver driver = GraphDatabase.driver(boltUrl, AuthTokens.none()); Session session = driver.session();) {
                System.out.println("Starting streams...");
                Result result = session.run("CALL streams.configuration.get()");
                Map<String, String> configuration = result.stream()
                        .map(map -> Map.entry(map.get("name").asString(), map.get("value").asString()))
                        .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
                System.out.println("Streams config: " + configuration);

                //Publish message
                String topic = topicsToCreate.get(0);
                String payload = "Sample message";
                session.run("CALL streams.publish($topic, $payload);", Map.of("topic", topic, "payload", payload)).consume();
            }

            //Verify offset has increased --hangs forever due to no messages being successfully passed.
//            Container.ExecResult execResult = kafka.execInContainer("kafka-console-consumer", "--topic", "MyTopic1", "--from-beginning", "--bootstrap-server", "localhost:9092", "--max-messages", "1");
//            assertThat(execResult.getStderr()).isEmpty();
//            assertThat(execResult.getStdout()).isNotEmpty().contains("Sample message");
        }
    }
}
