package com.yugabyte.cdc;

import static java.lang.Thread.sleep;

import com.google.common.net.HostAndPort;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.util.JsonFormat;
import com.stumbleupon.async.Callback;
import com.yugabyte.jdbc.source.SchemaMapping;
import com.yugabyte.jdbc.util.Version;
import java.sql.*;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.connect.data.*;
import org.apache.kafka.connect.data.Date;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.data.Time;
import org.apache.kafka.connect.data.Timestamp;
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
  private String tableId;
  private YBTable table;

  private AsyncYBClient client;
  private static YBClient syncClient;
  List<String> tabletIds = new ArrayList<>();
  List<HostAndPort> hostAndPorts = new ArrayList<>();

  protected SchemaMapping schemaMapping;

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
                config.getString(CDCSourceConnectorConfig.MASTER_ADDRESS_CONFIG))
            .defaultAdminOperationTimeoutMs(DEFAULT_TIMEOUT)
            .defaultOperationTimeoutMs(DEFAULT_TIMEOUT)
            .defaultSocketReadTimeoutMs(DEFAULT_TIMEOUT)
            .build();

    syncClient = new YBClient(client);

    tableId = config.getString(CDCSourceTaskConfig.TABLEID_CONFIG);
    streamId = config.getString(CDCSourceTaskConfig.STREAMID_CONFIG);

    try {
      table = syncClient.openTableByUUID(tableId);
      log.info("The table is " + table);
    } catch (Exception e) {
      e.printStackTrace();
    }

    List<String> tabletLocations = config.getList(CDCSourceTaskConfig.TABLET_LOCATION_CONFIG);
    for (String tabletLocation : tabletLocations) {
      String[] arr = tabletLocation.split(" ", 2);
      log.info("The tablet id is " + arr[0]);
      tabletIds.add(arr[0]);
      hostAndPorts.add(HostAndPort.fromString(arr[1]));
      log.info("The hostPort is " + HostAndPort.fromString(arr[1]));
      log.info("The hostport string is " + arr[1]);
    }
    // for each tablet call the get change
    this.running.set(true);
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
    final CountDownLatch latch = new CountDownLatch(tabletIds.size());
    log.info("The size of list of tabletIds is " + tabletIds.size());

    if (running.get()) {
      int index = 0;
      for (String id : tabletIds) {
        HostAndPort hp = hostAndPorts.get(index);
        log.info(
            "calling the getchanges for tabletid " + id + " hp " + hp + " table " + this.table);
        client.getChanges(
            hp,
            this.table,
            this.streamId,
            id,
            this.term,
            this.index,
            new Callback<Void, GetChangesResponse>() {
              @Override
              public Void call(GetChangesResponse getChangesResponse) throws Exception {
                try {
                  handlePoll(getChangesResponse, records);
                } finally {
                  latch.countDown();
                  log.info("latch countdown done.");
                }
                return null;
              }
            });
      }
      latch.await();
      sleep(1000);
      log.info("Wait for latch is over.");
    }
    return records;
  }

  private Schema buildSchema(List<CdcService.KeyValuePairPB> changes) {

    String schemaName = "record";
    SchemaBuilder builder = SchemaBuilder.struct().name(schemaName);
    changes.forEach(
        x -> {
          switch (x.getValue().getValueCase()) {
            case INT8_VALUE:
              builder.field(x.getKey().toStringUtf8(), Schema.BOOLEAN_SCHEMA);

              break;
            case INT16_VALUE:
              builder.field(x.getKey().toStringUtf8(), Schema.INT16_SCHEMA);
              break;
            case INT32_VALUE:
              builder.field(x.getKey().toStringUtf8(), Schema.INT32_SCHEMA);
              break;
            case INT64_VALUE:
              builder.field(x.getKey().toStringUtf8(), Schema.INT64_SCHEMA);
              break;
            case FLOAT_VALUE:
              builder.field(x.getKey().toStringUtf8(), Schema.FLOAT32_SCHEMA);
              break;
            case DOUBLE_VALUE:
              builder.field(x.getKey().toStringUtf8(), Schema.FLOAT64_SCHEMA);
              break;
            case STRING_VALUE:
              builder.field(x.getKey().toStringUtf8(), Schema.STRING_SCHEMA);
              break;
            case BOOL_VALUE:
              builder.field(x.getKey().toStringUtf8(), Schema.BOOLEAN_SCHEMA);
              break;
            case TIMESTAMP_VALUE:
              SchemaBuilder timeStampSchemaBuilder = Timestamp.builder();
              builder.field(x.getKey().toStringUtf8(), timeStampSchemaBuilder.build());
              break;
            case BINARY_VALUE:
              builder.field(x.getKey().toStringUtf8(), Schema.BYTES_SCHEMA);
              break;
            case INETADDRESS_VALUE:
              builder.field(x.getKey().toStringUtf8(), Schema.STRING_SCHEMA);
              break;
            case MAP_VALUE:
              builder.field(x.getKey().toStringUtf8(), Schema.STRING_SCHEMA);
              break;
            case SET_VALUE:
              builder.field(x.getKey().toStringUtf8(), Schema.STRING_SCHEMA);
              break;
            case LIST_VALUE:
              builder.field(x.getKey().toStringUtf8(), Schema.STRING_SCHEMA);
              break;
            case DECIMAL_VALUE:
              SchemaBuilder decimal = Decimal.builder(64);
              builder.field(x.getKey().toStringUtf8(), decimal);
              break;
            case VARINT_VALUE:
              builder.field(x.getKey().toStringUtf8(), Schema.STRING_SCHEMA);
              break;
            case FROZEN_VALUE:
              builder.field(x.getKey().toStringUtf8(), Schema.BYTES_SCHEMA);
              break;
            case UUID_VALUE:
              builder.field(x.getKey().toStringUtf8(), Schema.STRING_SCHEMA);
              break;
            case TIMEUUID_VALUE:
              builder.field(x.getKey().toStringUtf8(), Schema.STRING_SCHEMA);
              break;
            case JSONB_VALUE:
              builder.field(x.getKey().toStringUtf8(), Schema.STRING_SCHEMA);
              break;
            case DATE_VALUE:
              SchemaBuilder dateSchemaBuilder = Date.builder();
              builder.field(x.getKey().toStringUtf8(), dateSchemaBuilder.build());
              break;
            case TIME_VALUE:
              SchemaBuilder timeSchemaBuilder = Time.builder();
              builder.field(x.getKey().toStringUtf8(), timeSchemaBuilder.build());
              break;
            case UINT32_VALUE:
              builder.field(x.getKey().toStringUtf8(), Schema.INT32_SCHEMA);
              break;
            case UINT64_VALUE:
              builder.field(x.getKey().toStringUtf8(), Schema.INT64_SCHEMA);
              break;
            case VIRTUAL_VALUE:
              builder.field(x.getKey().toStringUtf8(), Schema.BYTES_SCHEMA);
              break;
            case VALUE_NOT_SET:
              log.info("Error , the value is not set.");
              break;
            default:
              log.info("Error , the value is not set.");
          }
          log.info("SKSK the schema record value is ");
        });

    return builder.build();
  }

  private Void handlePoll(GetChangesResponse getChangesResponse, List<SourceRecord> recordList) {
    // In case of exception deal with it and continue
    log.info("SK: handlePoll.");

    final String topic = this.table.getName();
    final Map<String, String> partition =
        Collections.singletonMap("tablename", this.table.getName());

    for (org.yb.cdc.CdcService.CDCRecord2PB record :
        getChangesResponse.getResp().getCDCRecordsList()) {
      if (record.getOperation().equals(CdcService.CDCRecord2PB.OperationType.DDL)) {
        // register the schema or save it locally
        log.info("The schema is " + record.getSchema());
        continue;
      }

      List<CdcService.KeyValuePairPB> primary_keys = record.getKeyList();
      List<CdcService.KeyValuePairPB> changes = record.getChangesList();

      Schema keySchema = buildSchema(primary_keys);
      Schema valueSchema = buildSchema(changes);

      log.info("The keySchema is " + keySchema);
      log.info("The valueSchema is " + valueSchema);

      Object keyValue = buildObject(primary_keys, keySchema);
      Object value = buildObject(changes, valueSchema);

      log.info("The keyValue is " + keyValue);
      log.info("The value is " + value);

      try {
        log.info("Printing the json format of CDCRecord2PB " + JsonFormat.printer().print(record));
      } catch (InvalidProtocolBufferException ie) {
        ie.printStackTrace();
      }
      primary_keys.forEach(
          x -> {
            log.info("SKSK the source record key is " + x.getKey().toStringUtf8());
          });

      if (record.getOperation().equals(CdcService.CDCRecord2PB.OperationType.WRITE)
          && record.getTransactionState() != null
          && record.getTransactionState().hasStatus()) {
        // register the schema or save it locally
        log.info("This is the commit transaction event. ignore it for now ");
        continue;
      }
      recordList.add(
          new SourceRecord(partition, null, topic, keySchema, keyValue, valueSchema, value));
    }

    log.info("The record list is " + recordList);
    // convert and add to recordList
    this.term = getChangesResponse.getResp().getCheckpoint().getOpId().getTerm();
    this.index = getChangesResponse.getResp().getCheckpoint().getOpId().getIndex();
    log.info("The term: " + this.term + " and index is " + this.index);
    return null;
  }

  private Object buildObject(List<CdcService.KeyValuePairPB> keyValuePairs, Schema schema) {
    Struct record = new Struct(schema);
    keyValuePairs.forEach(
        x -> {
          switch (x.getValue().getValueCase()) {
            case INT8_VALUE:
              record.put(x.getKey().toStringUtf8(), x.getValue().getInt8Value());
              break;
            case INT16_VALUE:
              record.put(x.getKey().toStringUtf8(), x.getValue().getInt16Value());
              break;
            case INT32_VALUE:
              record.put(x.getKey().toStringUtf8(), x.getValue().getInt32Value());
              break;
            case INT64_VALUE:
              record.put(x.getKey().toStringUtf8(), x.getValue().getInt64Value());
              break;
            case FLOAT_VALUE:
              record.put(x.getKey().toStringUtf8(), x.getValue().getFloatValue());
              break;
            case DOUBLE_VALUE:
              record.put(x.getKey().toStringUtf8(), x.getValue().getDoubleValue());
              break;
            case STRING_VALUE:
              record.put(x.getKey().toStringUtf8(), x.getValue().getStringValue());
              break;
            case BOOL_VALUE:
              record.put(x.getKey().toStringUtf8(), x.getValue().getBoolValue());
              break;
            case TIMESTAMP_VALUE:
              record.put(x.getKey().toStringUtf8(), x.getValue().getTimestampValue());
              break;
            case BINARY_VALUE:
              record.put(x.getKey().toStringUtf8(), x.getValue().getBinaryValue());
              break;
            case INETADDRESS_VALUE:
              record.put(x.getKey().toStringUtf8(), x.getValue().getInetaddressValue());
              break;
            case MAP_VALUE:
              record.put(x.getKey().toStringUtf8(), x.getValue().getMapValue());
              break;
            case SET_VALUE:
              record.put(x.getKey().toStringUtf8(), x.getValue().getSetValue());
              break;
            case LIST_VALUE:
              record.put(x.getKey().toStringUtf8(), x.getValue().getListValue());
              break;
            case DECIMAL_VALUE:
              record.put(x.getKey().toStringUtf8(), x.getValue().getDecimalValue());
              break;
            case VARINT_VALUE:
              record.put(x.getKey().toStringUtf8(), x.getValue().getVarintValue());
              break;
            case FROZEN_VALUE:
              record.put(x.getKey().toStringUtf8(), x.getValue().getFrozenValue());
              break;
            case UUID_VALUE:
              record.put(x.getKey().toStringUtf8(), x.getValue().getUuidValue());
              break;
            case TIMEUUID_VALUE:
              record.put(x.getKey().toStringUtf8(), x.getValue().getTimeuuidValue());
              break;
            case JSONB_VALUE:
              record.put(x.getKey().toStringUtf8(), x.getValue().getJsonbValue());
              break;
            case DATE_VALUE:
              record.put(x.getKey().toStringUtf8(), x.getValue().getDateValue());
              break;
            case TIME_VALUE:
              record.put(x.getKey().toStringUtf8(), x.getValue().getTimeValue());
              break;
            case UINT32_VALUE:
              record.put(x.getKey().toStringUtf8(), x.getValue().getUint32Value());
              break;
            case UINT64_VALUE:
              record.put(x.getKey().toStringUtf8(), x.getValue().getUint64Value());
              break;
            case VIRTUAL_VALUE:
              record.put(x.getKey().toStringUtf8(), x.getValue().getVirtualValue());
              break;
            case VALUE_NOT_SET:
              log.info("Error , the value is not set.");
              break;
            default:
              record.put(x.getKey().toStringUtf8(), null);
          }
        });
    return record;
  }

  @Override
  public void stop() {
    log.info("Stopping CDC source task");
    running.set(false);
  }
}
