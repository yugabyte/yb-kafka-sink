package com.yugabyte.cdc;

import com.google.common.net.HostAndPort;
import com.yugabyte.jdbc.dialect.DatabaseDialect;
import com.yugabyte.jdbc.util.CachedConnectionProvider;
import com.yugabyte.jdbc.util.Version;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.stream.Collectors;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.source.SourceConnector;
import org.apache.kafka.connect.util.ConnectorUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.yb.client.*;
import org.yb.master.Master;
import org.yb.util.ServerInfo;

public class YBCDCSourceConnector extends SourceConnector {

  private static final Logger log = LoggerFactory.getLogger(YBCDCSourceConnector.class);
  private static final long MAX_TIMEOUT = 10000L;

  private static final int DEFAULT_TIMEOUT = 30000;
  private YBTable table;
  public String streamId = "";

  private CDCSourceConnectorConfig config;
  private CachedConnectionProvider cachedConnectionProvider;

  private CDCTableMonitorThread tableMonitorThread;
  private DatabaseDialect dialect;
  private Map<String, String> configProperties;

  private static AsyncYBClient client;
  private static YBClient syncClient;

  List<HostAndPort> hps = new ArrayList<>();

  @Override
  public void start(Map<String, String> properties) {
    log.info("Starting CDC Source Connector");
    try {
      configProperties = properties;
      config = new CDCSourceConnectorConfig(configProperties);
    } catch (ConfigException e) {
      throw new ConnectException("Couldn't start CDCSourceConnector due to configuration error", e);
    }

    client =
        new AsyncYBClient.AsyncYBClientBuilder(
                config.getString(CDCSourceConnectorConfig.MASTER_ADDRESS_CONFIG))
            .defaultAdminOperationTimeoutMs(DEFAULT_TIMEOUT)
            .defaultOperationTimeoutMs(DEFAULT_TIMEOUT)
            .defaultSocketReadTimeoutMs(DEFAULT_TIMEOUT)
            .build();

    syncClient = new YBClient(client);

    String tableName = config.getString(CDCSourceConnectorConfig.TABLE_NAME_CONFIG);
    String namespaceName = config.getString(CDCSourceConnectorConfig.NAMESPACE_NAME_CONFIG);

    log.info("The table name is " + tableName);
    log.info("The namespace name is " + namespaceName);
    // get all the tables and then tablets
    String tableId = null;
    ListTablesResponse tablesResp = null;
    try {
      tablesResp = syncClient.getTablesList();
      for (Master.ListTablesResponsePB.TableInfo tableInfo : tablesResp.getTableInfoList()) {
        log.info("The tables are " + tableInfo.getName());
        if (tableInfo.getName().equals(tableName)
            && tableInfo.getNamespace().getName().equals(namespaceName)) {
          tableId = tableInfo.getId().toStringUtf8();
        }
      }
      table = syncClient.openTableByUUID(tableId);
      ListTabletServersResponse serversResp = syncClient.listTabletServers();
      for (ServerInfo serverInfo : serversResp.getTabletServersList()) {
        hps.add(HostAndPort.fromParts(serverInfo.getHost(), serverInfo.getPort()));
      }

      Random rand = new Random();
      HostAndPort hp = hps.get(rand.nextInt(hps.size()));
      if (streamId.isEmpty()) {
        streamId = syncClient.createCDCStream(hp, table.getTableId()).getStreamId();
        log.info(String.format("Created new stream with id %s", streamId));
      }
    } catch (Exception e) {
      e.printStackTrace();
    }
    if (tableId == null) {
      log.error(String.format("Could not find a table with name %s.%s", namespaceName, tableName));
      System.exit(0);
    }
  }

  public Class<? extends Task> taskClass() {
    return CDCSourceTask.class;
  }

  @Override
  public List<Map<String, String>> taskConfigs(int maxTasks) {
    List<LocatedTablet> tabletLocations = null;
    List<Map<String, String>> taskConfigs = null;
    try {
      tabletLocations = table.getTabletsLocations(DEFAULT_TIMEOUT);

      int numGroups = Math.min(hps.size(), maxTasks);
      List<List<LocatedTablet>> tabletsGrouped =
          ConnectorUtils.groupPartitions(tabletLocations, numGroups);
      taskConfigs = new ArrayList<>(tabletsGrouped.size());

      for (List<LocatedTablet> locatedTablets : tabletsGrouped) {
        Map<String, String> taskProps = new HashMap<>(configProperties);
        List<String> tabletHostList =
            locatedTablets
                .stream()
                .map(
                    t ->
                        new String(t.getTabletId(), StandardCharsets.UTF_8)
                            + " "
                            + t.getLeaderReplica().getRpcHost()
                            + ":"
                            + t.getLeaderReplica().getRpcPort())
                .collect(Collectors.toList());

        taskProps.put(CDCSourceTaskConfig.TABLET_LOCATION_CONFIG, String.join(",", tabletHostList));
        taskProps.put(CDCSourceTaskConfig.STREAMID_CONFIG, streamId);
        taskProps.put(CDCSourceTaskConfig.TABLEID_CONFIG, table.getTableId());
        taskConfigs.add(taskProps);
      }
    } catch (Exception e) {
      e.printStackTrace();
    }
    return taskConfigs;
  }

  @Override
  public void stop() {
    log.info("Stopping table monitoring thread");
    /*tableMonitorThread.shutdown();
    try {
      tableMonitorThread.join(MAX_TIMEOUT);
    } catch (InterruptedException e) {
      // Ignore, shouldn't be interrupted
    } finally {
      try {
        cachedConnectionProvider.close();
      } finally {
        try {
          if (dialect != null) {
            dialect.close();
          }
        } catch (Throwable t) {
          log.warn("Error while closing the {} dialect: ", dialect, t);
        } finally {
          dialect = null;
        }
      }
    }*/
  }

  @Override
  public ConfigDef config() {
    return CDCSourceConnectorConfig.CONFIG_DEF;
  }

  @Override
  public String version() {
    return Version.getVersion();
  }
}
