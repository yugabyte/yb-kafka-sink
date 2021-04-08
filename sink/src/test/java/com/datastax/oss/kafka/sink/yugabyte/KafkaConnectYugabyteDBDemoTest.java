package com.datastax.oss.kafka.sink.yugabyte;

import static io.restassured.RestAssured.given;
import static java.lang.String.format;
import static java.util.Collections.singletonList;
import static org.awaitility.Awaitility.await;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;

import com.datastax.oss.kafka.sink.yugabyte.kafkaconnect.ConnectorConfiguration;
import com.datastax.oss.kafka.sink.yugabyte.testcontainers.KafkaConnectContainer;
import com.datastax.oss.kafka.sink.yugabyte.testcontainers.KafkaContainer;
import com.datastax.oss.kafka.sink.yugabyte.testcontainers.SchemaRegistryContainer;
import com.datastax.oss.kafka.sink.yugabyte.testcontainers.ZookeeperContainer;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.findinpath.avro.BookmarkEvent;
import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig;
import io.confluent.kafka.serializers.KafkaAvroDeserializerConfig;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import io.confluent.kafka.serializers.KafkaAvroSerializerConfig;
import io.confluent.kafka.serializers.subject.TopicNameStrategy;
import io.confluent.kafka.streams.serdes.avro.GenericAvroDeserializer;
import io.restassured.http.ContentType;
import java.io.IOException;
import java.sql.*;
import java.time.Duration;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.avro.generic.GenericRecord;
import org.apache.http.HttpStatus;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.KafkaAdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.Testcontainers;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.lifecycle.Startables;

public class KafkaConnectYugabyteDBDemoTest {
  private static final Logger LOGGER =
      LoggerFactory.getLogger(KafkaConnectYugabyteDBDemoTest.class);
  private static final long POLL_INTERVAL_MS = 100L;
  private static final long POLL_TIMEOUT_MS = 20_000L;

  private static final String KAFKA_CONNECT_SOURCE_CONNECTOR_NAME = "bookmarkssource";
  private static final String KAFKA_CONNECT_CQLSINK_CONNECTOR_NAME = "cqlbookmarkssink";
  private static final String KAFKA_CONNECT_JDBCSINK_CONNECTOR_NAME = "jdbcbookmarkssink";

  private static final String KAFKA_CONNECT_OUTPUT_TOPIC = "findinpath.bookmarks";

  private static final String POSTGRES_DB_NAME = "yugabyte"; // ""findinpath";
  private static final String POSTGRES_NETWORK_ALIAS =
      "host.testcontainers.internal"; // "postgres";

  private static final String POSTGRES_DB_USERNAME = "yugabyte"; // ""sa";
  private static final String POSTGRES_DB_PASSWORD = "yugabyte"; // "p@ssw0rd!";
  private static final int POSTGRES_INTERNAL_PORT = 5433; // 5432;
  // jdbc:postgresql://127.0.0.1:5433/yugabyte
  /** Postgres JDBC connection URL to be used within the docker environment. */
  private static final String POSTGRES_INTERNAL_CONNECTION_URL =
      format(
          "jdbc:postgresql://%s:%d/%s", // ?loggerLevel=OFF
          POSTGRES_NETWORK_ALIAS, POSTGRES_INTERNAL_PORT, POSTGRES_DB_NAME);

  private static Network network;

  private static ZookeeperContainer zookeeperContainer;
  private static KafkaContainer kafkaContainer;
  private static SchemaRegistryContainer schemaRegistryContainer;
  private static KafkaConnectContainer kafkaConnectContainer;

  private static PostgreSQLContainer postgreSQLContainer;

  private static final ObjectMapper mapper = new ObjectMapper();

  /**
   * Bootstrap the docker instances needed for interracting with :
   *
   * <ul>
   *   <li>Confluent Kafka ecosystem
   *   <li>PostgreSQL
   * </ul>
   */
  @BeforeAll
  public static void dockerSetup() throws Exception {
    network = Network.newNetwork();
    int cqlPort = 9042;
    int jdbcPort = 5433;

    final String rootUrl = String.format("http://host.testcontainers.internal:%d/", cqlPort);
    Testcontainers.exposeHostPorts(cqlPort);
    Testcontainers.exposeHostPorts(jdbcPort);

    // Confluent setup
    zookeeperContainer = new ZookeeperContainer().withNetwork(network);
    kafkaContainer = new KafkaContainer(zookeeperContainer.getInternalUrl()).withNetwork(network);
    schemaRegistryContainer =
        new SchemaRegistryContainer(zookeeperContainer.getInternalUrl()).withNetwork(network);
    kafkaConnectContainer =
        new KafkaConnectContainer(kafkaContainer.getInternalBootstrapServersUrl())
            .withNetwork(network)
            .withPlugins(
                "plugins/kafka-connect-ycql/kafka-connect-yugabytedb-sink-1.4.1-SNAPSHOT.jar")
            .withPlugins("plugins/kafka-connect-ycql/postgresql-42.2.12.jar")
            .withKeyConverter("org.apache.kafka.connect.storage.StringConverter")
            .withValueConverter("io.confluent.connect.avro.AvroConverter")
            .withSchemaRegistryUrl(schemaRegistryContainer.getInternalUrl());

    // Postgres setup
    /*postgreSQLContainer = new PostgreSQLContainer<>("postgres:12")
    .withNetwork(network)
    .withNetworkAliases(POSTGRES_NETWORK_ALIAS)
    .withInitScript("postgres/init_postgres.sql")
    .withDatabaseName(POSTGRES_DB_NAME)
    .withUsername(POSTGRES_DB_USERNAME)
    .withPassword(POSTGRES_DB_PASSWORD);*/

    Startables.deepStart(
            Stream.of(
                zookeeperContainer,
                kafkaContainer,
                schemaRegistryContainer,
                kafkaConnectContainer /*,
                                      postgreSQLContainer*/))
        .join();

    verifyKafkaConnectHealth();

    // For Sink
    createTopics("findinpath");
    registerSchemaRegistryTypes();
  }

  @Test
  public void testYugabyteDBKafkaSink() throws InterruptedException {
    ConnectorConfiguration bookmarksCQLSinkConnectorConfig =
        createYugabyteCQLSinkConnectorConfig(
            KAFKA_CONNECT_CQLSINK_CONNECTOR_NAME,
            POSTGRES_INTERNAL_CONNECTION_URL,
            POSTGRES_DB_USERNAME,
            POSTGRES_DB_PASSWORD);
    registerBookmarksTableConnector(bookmarksCQLSinkConnectorConfig);

    ConnectorConfiguration bookmarksJDBCSinkConnectorConfig =
        createYugabyteJDBCSinkConnectorConfig(
            KAFKA_CONNECT_JDBCSINK_CONNECTOR_NAME,
            POSTGRES_INTERNAL_CONNECTION_URL,
            POSTGRES_DB_USERNAME,
            POSTGRES_DB_PASSWORD);
    registerBookmarksTableConnector(bookmarksJDBCSinkConnectorConfig);

    final UUID userUuid = UUID.randomUUID();
    final com.findinpath.avro.BookmarkEvent bookmarkEvent =
        new com.findinpath.avro.BookmarkEvent(
            userUuid.toString(), "www.yugabyte.com", Instant.now().toEpochMilli());

    String topic = "findinpath";
    produce(topic, bookmarkEvent);
    LOGGER.info(
        String.format("Successfully sent 1 BookmarkEvent message to the topic called %s", topic));

    List<ConsumerRecord<String, com.findinpath.avro.BookmarkEvent>> consumerRecords =
        dumpGenericRecordTopic(topic, 1, POLL_TIMEOUT_MS);
    LOGGER.info(
        String.format(
            "Retrieved %d consumer records from the topic %s", consumerRecords.size(), topic));

    assertThat(consumerRecords.size(), equalTo(1));
    assertThat(consumerRecords.get(0).key(), equalTo(bookmarkEvent.getUserUuid()));
    consumerRecords.forEach(r -> LOGGER.info("The record is " + r.value()));
    // assertThat(consumerRecords.get(0).value(), equalTo(bookmarkEvent));

    Thread.sleep(30000);
    // check if the database has this row
    String tablename = topic;
    awaitForRowInYugabyteDB(tablename, bookmarkEvent, 360, TimeUnit.SECONDS);
  }

  private void awaitForRowInYugabyteDB(
      String tablename, BookmarkEvent bookmarkEvent, int timeout, TimeUnit timeUnit) {

    try (Connection conn = connect();
        Statement stmt = conn.createStatement()) {
      await()
          .atMost(timeout, timeUnit)
          .pollInterval(Duration.ofMillis(100))
          .until(
              () -> {
                ResultSet rs = stmt.executeQuery("select * from " + tablename);
                BookmarkEvent event = null;
                while (rs.next()) {
                  event =
                      new BookmarkEvent(
                          rs.getString("userUuid"), rs.getString("url"), rs.getLong("timestamp"));
                  LOGGER.info(
                      "SKSKSK The event is " + event + "and the bookmarevent is " + bookmarkEvent);
                }
                return bookmarkEvent.equals(event);
              });
    } catch (SQLException e) {
      throw new RuntimeException("Exception occurred while checking rows in the table ", e);
    }
  }

  private static void produce(String topic, com.findinpath.avro.BookmarkEvent bookmarkEvent) {
    try (KafkaProducer<String, com.findinpath.avro.BookmarkEvent> producer =
        createBookmarkEventKafkaProducer()) {
      final ProducerRecord<String, com.findinpath.avro.BookmarkEvent> record =
          new ProducerRecord<>(topic, bookmarkEvent.getUserUuid().toString(), bookmarkEvent);
      producer.send(record);
      producer.flush();
    } catch (final SerializationException e) {
      LOGGER.error(
          String.format(
              "Serialization exception occurred while trying to send message %s to the topic %s",
              bookmarkEvent, topic),
          e);
    }
  }

  private static KafkaProducer<String, com.findinpath.avro.BookmarkEvent>
      createBookmarkEventKafkaProducer() {
    final Properties props = new Properties();
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaContainer.getBootstrapServersUrl());
    props.put(ProducerConfig.ACKS_CONFIG, "all");
    props.put(ProducerConfig.RETRIES_CONFIG, 0);
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class);
    props.put(
        AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, schemaRegistryContainer.getUrl());
    props.put(
        KafkaAvroSerializerConfig.VALUE_SUBJECT_NAME_STRATEGY, TopicNameStrategy.class.getName());

    return new KafkaProducer<>(props);
  }

  private static void createTopics(String topic) throws InterruptedException, ExecutionException {
    try (AdminClient adminClient = createAdminClient()) {
      short replicationFactor = 1;
      int partitions = 1;

      LOGGER.info("Creating topics in Apache Kafka");
      adminClient
          .createTopics(singletonList(new NewTopic(topic, partitions, replicationFactor)))
          .all()
          .get();
    }
  }

  private static void registerSchemaRegistryTypes() throws IOException, RestClientException {
    // the type BookmarkEvent needs to be registered manually in the Confluent schema registry
    // to setup the tests.
    LOGGER.info("Registering manually in the Schema Registry the types used in the tests");
    CachedSchemaRegistryClient schemaRegistryClient =
        new CachedSchemaRegistryClient(schemaRegistryContainer.getUrl(), 1000);
    schemaRegistryClient.register(
        com.findinpath.avro.BookmarkEvent.getClassSchema().getFullName(),
        com.findinpath.avro.BookmarkEvent.getClassSchema());
  }

  private void createTable() {
    String sql =
        "CREATE TABLE IF NOT EXISTS bookmarks(\n"
            + "    id bigserial,\n"
            + "    name varchar(256),\n"
            + "    url varchar(1024),\n"
            + "    updated TIMESTAMP WITHOUT TIME ZONE DEFAULT timezone('utc' :: TEXT, now()),\n"
            + "    primary key (id)\n"
            + ")";
    try (Connection conn = connect();
        Statement stmt = conn.createStatement()) {
      stmt.execute(sql);
      stmt.execute("TRUNCATE TABLE bookmarks");
    } catch (SQLException e) {
      throw new RuntimeException("Exception occurred while creating table ", e);
    }
  }

  /**
   * Utility method for dumping the AVRO contents of a Apache Kafka topic.
   *
   * @param topic the topic name
   * @param minMessageCount the amount of messages to wait for before completing the operation
   * @param pollTimeoutMillis the period to wait until throwing a timeout exception
   * @param <T>
   * @return the generic records contained in the specified topic.
   */
  private <T extends GenericRecord> List<ConsumerRecord<String, T>> dumpGenericRecordTopic(
      String topic, int minMessageCount, long pollTimeoutMillis) {
    List<ConsumerRecord<String, T>> consumerRecords = new ArrayList<>();
    String consumerGroupId = UUID.randomUUID().toString();
    try (final KafkaConsumer<String, T> consumer =
        createGenericRecordKafkaConsumer(consumerGroupId)) {
      // assign the consumer to all the partitions of the topic
      List<TopicPartition> topicPartitions =
          consumer
              .partitionsFor(topic)
              .stream()
              .map(partitionInfo -> new TopicPartition(topic, partitionInfo.partition()))
              .collect(Collectors.toList());
      consumer.assign(topicPartitions);
      long start = System.currentTimeMillis();
      while (true) {
        final ConsumerRecords<String, T> records =
            consumer.poll(Duration.ofMillis(POLL_INTERVAL_MS));
        records.forEach(consumerRecords::add);
        if (consumerRecords.size() >= minMessageCount) {
          break;
        }
        if (System.currentTimeMillis() - start > pollTimeoutMillis) {
          throw new IllegalStateException(
              format(
                  "Timed out while waiting for %d messages from the %s. Only %d messages received so far.",
                  minMessageCount, topic, consumerRecords.size()));
        }
      }
    }
    return consumerRecords;
  }

  private static <T extends GenericRecord>
      KafkaConsumer<String, T> createGenericRecordKafkaConsumer(String consumerGroupId) {
    final Properties props = new Properties();
    props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaContainer.getBootstrapServersUrl());
    props.put(ConsumerConfig.GROUP_ID_CONFIG, consumerGroupId);
    props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
    props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "1000");
    props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    props.put(
        AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, schemaRegistryContainer.getUrl());
    props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
    props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, GenericAvroDeserializer.class);
    props.put(KafkaAvroDeserializerConfig.SPECIFIC_AVRO_READER_CONFIG, false);
    props.put(
        KafkaAvroSerializerConfig.VALUE_SUBJECT_NAME_STRATEGY, TopicNameStrategy.class.getName());
    return new KafkaConsumer<>(props);
  }

  /**
   * Wait at most until the timeout, for the creation of the specified <code>topicName</code>.
   *
   * @param topicName the Apache Kafka topic name for which it is expected the creation
   * @param timeout the amount of time units to expect until the operation is ca
   * @param timeUnit the time unit a timeout exception will be thrown
   */
  private static void awaitForTopicCreation(String topicName, long timeout, TimeUnit timeUnit) {
    try (AdminClient adminClient = createAdminClient()) {
      await()
          .atMost(timeout, timeUnit)
          .pollInterval(Duration.ofMillis(100))
          .until(
              () -> {
                return adminClient.listTopics().names().get().contains(topicName);
              });
    }
  }

  /**
   * Creates a utility class for interacting for administrative purposes with Apache Kafka.
   *
   * @return the admin client.
   */
  private static AdminClient createAdminClient() {
    Properties properties = new Properties();
    properties.put(
        ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaContainer.getBootstrapServersUrl());

    return KafkaAdminClient.create(properties);
  }

  /**
   * Retrieves a PostgreSQL database connection
   *
   * @return a database connection
   * @throws SQLException wraps the exceptions which may occur
   */
  private Connection connect() throws SQLException {
    return DriverManager.getConnection(
        "jdbc:postgresql://127.0.0.1:5433/yugabyte" /*postgreSQLContainer.getJdbcUrl()*/,
        POSTGRES_DB_USERNAME,
        POSTGRES_DB_PASSWORD);
  }

  /**
   * Wraps the JDBC complexity needed for inserting into PostgreSQL a new Bookmark entry.
   *
   * @param bookmark the bookmark to be inserted.
   * @return the ID of the inserted bookmark in the database.
   */
  private long insertBookmark(Bookmark bookmark) {
    String SQL = "INSERT INTO bookmarks(name,url) VALUES (?,?)";

    try (Connection conn = connect();
        PreparedStatement pstmt = conn.prepareStatement(SQL, Statement.RETURN_GENERATED_KEYS)) {

      pstmt.setString(1, bookmark.getName());
      pstmt.setString(2, bookmark.getUrl());

      int affectedRows = pstmt.executeUpdate();
      // check the affected rows
      if (affectedRows > 0) {
        // get the ID back
        try (ResultSet rs = pstmt.getGeneratedKeys()) {
          if (rs.next()) {
            return rs.getLong(1);
          }
        } catch (SQLException e) {
          throw new RuntimeException("Exception occurred while inserting " + bookmark, e);
        }
      }
    } catch (SQLException e) {
      throw new RuntimeException("Exception occurred while inserting " + bookmark, e);
    }

    return 0;
  }

  private static ConnectorConfiguration createYugabyteJDBCSinkConnectorConfig(
      String connectorName,
      String connectionUrl,
      String connectionUser,
      String connectionPassword) {
    Map config = new HashMap<String, String>();
    config.put("name", connectorName);
    config.put("connector.class", "com.yugabyte.jdbc.JdbcSinkConnector");
    config.put("tasks.max", "1");
    config.put("connection.urls", connectionUrl + "," + connectionUrl);
    config.put("connection.user", connectionUser);
    config.put("connection.password", connectionPassword);
    config.put("mode", "UPSERT");
    config.put("auto.create", "true");
    config.put("topics", "findinpath");
    return new ConnectorConfiguration(connectorName, config);
  }

  private static ConnectorConfiguration createYugabyteCQLSinkConnectorConfig(
      String connectorName,
      String connectionUrl,
      String connectionUser,
      String connectionPassword) {
    Map config = new HashMap<String, String>();
    config.put("name", connectorName);
    config.put("connector.class", "com.datastax.oss.kafka.sink.CassandraSinkConnector");
    config.put("tasks.max", "1");
    config.put("contactPoints", "host.testcontainers.internal");
    config.put("loadBalancing.localDc", "datacenter1");
    config.put("port", "9042");
    config.put("auth.username", "cassandra");
    config.put("auth.password", "cassandra");
    config.put("topics", "findinpath");
    // topic.my_topic.demo.world_table.mapping=recordid=key, continent=value
    config.put(
        "topic.findinpath.demo.bookmark.mapping",
        "useruuid=key, url=value.url, timestamp=value.timestamp");
    return new ConnectorConfiguration(connectorName, config);
  }

  private static void registerBookmarksTableConnector(
      ConnectorConfiguration bookmarksTableConnectorConfig) {
    given()
        .log()
        .all()
        .contentType(ContentType.JSON)
        .accept(ContentType.JSON)
        .body(toJson(bookmarksTableConnectorConfig))
        .when()
        .post(kafkaConnectContainer.getUrl() + "/connectors")
        .andReturn()
        .then()
        .log()
        .all()
        .statusCode(HttpStatus.SC_CREATED);
  }

  private static String toJson(Object value) {
    try {
      return mapper.writeValueAsString(value);
    } catch (JsonProcessingException e) {
      throw new RuntimeException(e);
    }
  }

  /** Simple HTTP check to see that the kafka-connect server is available. */
  private static void verifyKafkaConnectHealth() {
    given()
        .log()
        .headers()
        .contentType(ContentType.JSON)
        .when()
        .get(kafkaConnectContainer.getUrl())
        .andReturn()
        .then()
        .log()
        .all()
        .statusCode(HttpStatus.SC_OK);
  }
}
