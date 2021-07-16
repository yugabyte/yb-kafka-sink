package com.yugabyte.cdc;

import java.util.Map;
import org.apache.kafka.common.config.ConfigDef;

/**
 * Configuration options for a single JdbcSourceTask. These are processed after all Connector-level
 * configs have been parsed.
 */
public class CDCSourceTaskConfig extends CDCSourceConnectorConfig {

  public static final String TABLEID_CONFIG = "table";
  private static final String TABLEID_DOC = "List of table id for this task to watch for changes.";

  /*  public static final String TABLETIDS_CONFIG = "tabletids";
      private static final String TABLETIDS_DOC = "List of tabletids for this task to watch for changes.";

      public static final String REPLICAS_CONFIG = "replicas";
      private static final String REPLICAS_DOC = "List of replicas for the tablets to watch for changes.";
  */
  public static final String STREAMID_CONFIG = "streamid";
  private static final String STREAMID_DOC = "streamid";

  public static final String TABLET_LOCATION_CONFIG = "tabletlocations";
  private static final String TABLET_LOCATION_DOC = "tabletlocations";

  static ConfigDef config =
      baseConfigDef()
          .define(TABLEID_CONFIG, ConfigDef.Type.STRING, ConfigDef.Importance.HIGH, TABLEID_DOC)
          // .define(TABLETIDS_CONFIG, ConfigDef.Type.LIST, ConfigDef.Importance.HIGH,
          // TABLETIDS_DOC)
          // .define(REPLICAS_CONFIG, ConfigDef.Type.LIST, ConfigDef.Importance.HIGH,
          // REPLICAS_DOC)*/
          .define(STREAMID_CONFIG, ConfigDef.Type.STRING, ConfigDef.Importance.HIGH, STREAMID_DOC)
          .define(
              TABLET_LOCATION_CONFIG,
              ConfigDef.Type.CLASS,
              ConfigDef.Importance.HIGH,
              TABLET_LOCATION_DOC);

  public CDCSourceTaskConfig(Map<String, String> props) {
    super(config, props);
  }
}
