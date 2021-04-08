package com.datastax.oss.kafka.sink.yugabyte.testcontainers;

import org.testcontainers.containers.GenericContainer;

public class YugabyteDBContainer extends GenericContainer<YugabyteDBContainer> {
  private static final int ZOOKEEPER_INTERNAL_PORT = 2181;
  private static final int ZOOKEEPER_TICK_TIME = 2000;

  private final String networkAlias = "zookeeper";
}
