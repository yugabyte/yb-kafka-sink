/*
 * Copyright 2018 Confluent Inc.
 *
 * Licensed under the Confluent Community License (the "License"); you may not use
 * this file except in compliance with the License.  You may obtain a copy of the
 * License at
 *
 * http://www.confluent.io/confluent-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OF ANY KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations under the License.
 */

package com.yugabyte.jdbc.sink;

import com.yugabyte.jdbc.dialect.DatabaseDialect;
import com.yugabyte.jdbc.dialect.DatabaseDialects;
import com.yugabyte.jdbc.util.Version;
import java.sql.SQLException;
import java.util.Collection;
import java.util.Map;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.errors.RetriableException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class JdbcSinkTask extends SinkTask {
  private static final Logger log = LoggerFactory.getLogger(JdbcSinkTask.class);

  DatabaseDialect dialect;
  JdbcSinkConfig config;
  JdbcDbWriter writer;
  int remainingRetries;

  @Override
  public void start(final Map<String, String> props) {
    log.info("Starting JDBC Sink task");
    config = new JdbcSinkConfig(props);
    initWriter();
    remainingRetries = config.maxRetries;
  }

  void initWriter() {
    dialect = DatabaseDialects.create("PostgreSqlDatabaseDialect", config);
    final DbStructure dbStructure = new DbStructure(dialect);
    log.info("Initializing writer using SQL dialect: {}", dialect.getClass().getSimpleName());
    writer = new JdbcDbWriter(config, dialect, dbStructure);
  }

  @Override
  public void put(Collection<SinkRecord> records) {
    if (records.isEmpty()) {
      return;
    }
    final SinkRecord first = records.iterator().next();
    final int recordsCount = records.size();
    log.debug(
        "Received {} records. First record kafka coordinates:({}-{}-{}). Writing them to the "
            + "database...",
        recordsCount,
        first.topic(),
        first.kafkaPartition(),
        first.kafkaOffset());
    try {
      long start = System.currentTimeMillis();
      writer.write(records);
      long end = System.currentTimeMillis();
      log.info(
          "The total time taken to write the batch({}-{}-{}) is : {} ms",
          first.topic(),
          first.kafkaPartition(),
          first.kafkaOffset(),
          (end - start));

    } catch (SQLException sqle) {
      log.warn(
          "Write of {} records failed, remainingRetries={}",
          records.size(),
          remainingRetries,
          sqle);
      String sqleAllMessages = "Exception chain:" + System.lineSeparator();
      int totalExceptions = 0;
      for (Throwable e : sqle) {
        sqleAllMessages += e + System.lineSeparator();
        totalExceptions++;
      }
      SQLException sqlAllMessagesException = new SQLException(sqleAllMessages);
      sqlAllMessagesException.setNextException(sqle);
      if (remainingRetries == 0) {
        log.error(
            "Failing task after exhausting retries; "
                + "encountered {} exceptions on last write attempt. "
                + "For complete details on each exception, please enable DEBUG logging.",
            totalExceptions);
        int exceptionCount = 1;
        for (Throwable e : sqle) {
          log.debug("Exception {}:", exceptionCount++, e);
        }
        throw new ConnectException(sqlAllMessagesException);
      } else {
        writer.closeQuietly();
        initWriter();
        remainingRetries--;
        context.timeout(config.retryBackoffMs);
        throw new RetriableException(sqlAllMessagesException);
      }
    }
    remainingRetries = config.maxRetries;
  }

  @Override
  public void flush(Map<TopicPartition, OffsetAndMetadata> map) {
    // Not necessary
  }

  public void stop() {
    log.info("Stopping task");
    try {
      writer.closeQuietly();
    } finally {
      try {
        if (dialect != null) {
          dialect.close();
        }
      } catch (Throwable t) {
        log.warn("Error while closing the {} dialect: ", dialect.name(), t);
      } finally {
        dialect = null;
      }
    }
  }

  @Override
  public String version() {
    return Version.getVersion();
  }
}
