package com.datastax.oss.kafka.sink.yugabyte;

import static java.lang.String.format;

import java.io.File;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import org.junit.ClassRule;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.Testcontainers;
import org.testcontainers.containers.DockerComposeContainer;

public class YugabyteContainerTest {

  private static final Logger LOGGER = LoggerFactory.getLogger(YugabyteContainerTest.class);
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

  @ClassRule
  public static DockerComposeContainer environment =
      new DockerComposeContainer(new File("src/test/docker-compose-yugabyte.yml"))
          .withExposedService("yb-tserver", 5433);

  @Test
  public void testYugabyteClient() {
    int localServerPort = 5433;
    final String rootUrl =
        String.format("http://host.testcontainers.internal:%d/", localServerPort);
    Testcontainers.exposeHostPorts(localServerPort);
    try {
      Thread.sleep(50000);
    } catch (InterruptedException e) {
      e.printStackTrace();
    }
  }

  private Connection connect() throws SQLException {
    return DriverManager.getConnection(
        "jdbc:postgresql://127.0.0.1:5433/yugabyte" /*postgreSQLContainer.getJdbcUrl()*/,
        POSTGRES_DB_USERNAME,
        POSTGRES_DB_PASSWORD);
  }
}
