package com.yugabyte.cdc;

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

import com.yugabyte.jdbc.source.JdbcSourceTaskConfig;
import java.time.ZoneId;
import java.util.*;
import java.util.regex.Pattern;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CDCSourceConnectorConfig extends AbstractConfig {

  private static final Logger LOG =
      LoggerFactory.getLogger(com.yugabyte.cdc.CDCSourceConnectorConfig.class);
  private static Pattern INVALID_CHARS = Pattern.compile("[^a-zA-Z0-9._-]");

  public static final String CONNECTION_PREFIX = "yugabyte.";
  public static final String CONNECTOR_GROUP = "Connector";

  public static final String NAMESPACE_NAME_DEFAULT = "yugabyte";
  public static final String NAMESPACE_NAME_CONFIG = "namespace.name";
  public static final String NAMESPACE_NAME_DOC = "";
  public static final String NAMESPACE_NAME_DISPLAY = "";

  public static final String MASTER_ADDRESS_DEFAULT = "127.0.0.1:7100";
  public static final String MASTER_ADDRESS_CONFIG = "master.address";
  private static final String MASTER_ADDRESS_DOC =
      "By default, the CDC connector will connect to local address";
  private static final String MASTER_ADDRESS_DISPLAY = "Master Address";

  public static final String TABLE_NAME_DEFAULT = "";
  public static final String TABLE_NAME_CONFIG = "table.name";
  private static final String TABLE_NAME_DOC =
      "By default, the CDC connector will connect to table name";
  private static final String TABLE_NAME_DISPLAY = "Table Types";

  public static ConfigDef baseConfigDef() {
    ConfigDef config = new ConfigDef();
    addConnectorOptions(config);
    return config;
  }

  private static final void addConnectorOptions(ConfigDef config) {
    LOG.info("SKSK the conifgs are added.");
    int orderInGroup = 0;
    config
        .define(
            TABLE_NAME_CONFIG,
            Type.STRING,
            TABLE_NAME_DEFAULT,
            Importance.HIGH,
            TABLE_NAME_DOC,
            CONNECTOR_GROUP,
            ++orderInGroup,
            Width.MEDIUM,
            TABLE_NAME_DISPLAY)
        .define(
            MASTER_ADDRESS_CONFIG,
            Type.STRING,
            MASTER_ADDRESS_DEFAULT,
            Importance.LOW,
            MASTER_ADDRESS_DOC,
            CONNECTOR_GROUP,
            ++orderInGroup,
            Width.MEDIUM,
            MASTER_ADDRESS_DISPLAY)
        .define(
            NAMESPACE_NAME_CONFIG,
            Type.STRING,
            NAMESPACE_NAME_DEFAULT,
            Importance.LOW,
            NAMESPACE_NAME_DOC,
            CONNECTOR_GROUP,
            ++orderInGroup,
            Width.MEDIUM,
            NAMESPACE_NAME_DISPLAY);
  }

  public static final ConfigDef CONFIG_DEF = baseConfigDef();

  public CDCSourceConnectorConfig(Map<String, ?> props) {
    super(CONFIG_DEF, props);
  }

  protected CDCSourceConnectorConfig(ConfigDef subclassConfigDef, Map<String, String> props) {
    super(subclassConfigDef, props);
  }

  public TimeZone timeZone() {
    String dbTimeZone = getString(JdbcSourceTaskConfig.DB_TIMEZONE_CONFIG);
    return TimeZone.getTimeZone(ZoneId.of(dbTimeZone));
  }

  public static void main(String[] args) {
    System.out.println(CONFIG_DEF.toEnrichedRst());
  }
}
