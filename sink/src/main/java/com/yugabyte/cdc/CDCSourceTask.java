package com.yugabyte.cdc;

import com.google.common.net.HostAndPort;
import com.stumbleupon.async.Callback;
import com.yugabyte.jdbc.util.Version;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.yb.cdc.CdcService;
import org.yb.client.AsyncYBClient;
import org.yb.client.GetChangesResponse;
import org.yb.client.YBClient;
import org.yb.client.YBTable;

public class CDCSourceTask extends SourceTask {

  private static final int DEFAULT_TIMEOUT = 30000;

  private static final Logger log = LoggerFactory.getLogger(CDCSourceTask.class);
  private final AtomicBoolean running = new AtomicBoolean(false);
  private CDCSourceTaskConfig config;

  private String streamId;
  private String tabletId;
  private String tableId;
  private YBTable table;

  private AsyncYBClient client;
  private static YBClient syncClient;
  List<String> tabletIds = new ArrayList<>();
  List<HostAndPort> hostAndPorts = new ArrayList<>();

  long term = 0;
  long index = 0;

  @Override
  public String version() {
    return Version.getVersion();
  }

  @Override
  public void start(Map<String, String> properties) {
    Map<String, String> configProperties;

    log.info("Starting CDC Source Connector");
    try {
      configProperties = properties;
      config = new CDCSourceTaskConfig(configProperties);
    } catch (ConfigException e) {
      throw new ConnectException("Couldn't start CDCSourceConnector due to configuration error", e);
    }

    client =
        new AsyncYBClient.AsyncYBClientBuilder(
                config.getString(CDCSourceConnectorConfig.MASTER_ADDRESS_DEFAULT))
            .defaultAdminOperationTimeoutMs(DEFAULT_TIMEOUT)
            .defaultOperationTimeoutMs(DEFAULT_TIMEOUT)
            .defaultSocketReadTimeoutMs(DEFAULT_TIMEOUT)
            .build();

    syncClient = new YBClient(client);

    tableId = config.getString(CDCSourceTaskConfig.TABLEID_CONFIG);
    streamId = config.getString(CDCSourceTaskConfig.STREAMID_CONFIG);

    try {
      table = syncClient.openTableByUUID(tableId);
    } catch (Exception e) {
      e.printStackTrace();
    }

    List<String> tabletLocations = config.getList(CDCSourceTaskConfig.TABLET_LOCATION_CONFIG);
    for (String tabletLocation : tabletLocations) {
      String[] arr = tabletLocation.split(" ");
      tabletIds.add(arr[0]);
      hostAndPorts.add(HostAndPort.fromString(arr[1]));
    }
    // for each tablet call the get change

  }

  @Override
  public List<SourceRecord> poll() throws InterruptedException {
    List<SourceRecord> records = new ArrayList<>();
    List<CdcService.CDCRecordPB> sourceRecords = new ArrayList<>();
    /*TopicMapper topicMapper = sourceConfig.getTopicMapper();

    SchemaAndValueProducer keySchemaAndValueProducer =
            SchemaAndValueProducers.createKeySchemaAndValueProvider(sourceConfig);
    SchemaAndValueProducer valueSchemaAndValueProducer =
            SchemaAndValueProducers.createValueSchemaAndValueProvider(sourceConfig);*/

      while (running.get()) {
      int index = 0;
      for (String id : tabletIds) {
        HostAndPort hp = hostAndPorts.get(index);
        client.getChanges(
            hp,
            table,
            streamId,
            id,
            term,
            index,
            new Callback<Void, GetChangesResponse>() {
              @Override
              public Void call(GetChangesResponse getChangesResponse) throws Exception {
                return handlePoll(getChangesResponse, records);
              }
            });
      }
    }
    return null;
  }

  private Void handlePoll(GetChangesResponse getChangesResponse, List<SourceRecord> recordList) {
    // In case of exception deal with it and continue

    for (org.yb.cdc.CdcService.CDCRecordPB record : getChangesResponse.getResp().getRecordsList()) {
      // convert to SourceRecord
      // recordList.add(record);
      log.info("SKSK the source record is " + record.toString());
    }
    this.term = getChangesResponse.getResp().getCheckpoint().getOpId().getTerm();
    this.index = getChangesResponse.getResp().getCheckpoint().getOpId().getIndex();
    return null;
  }

  @Override
  public void stop() {
    log.info("Stopping CDC source task");
    running.set(false);
  }
}
