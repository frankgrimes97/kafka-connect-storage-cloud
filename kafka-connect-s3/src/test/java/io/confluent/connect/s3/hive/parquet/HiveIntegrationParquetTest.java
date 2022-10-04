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

package io.confluent.connect.s3.hive.parquet;

import io.confluent.connect.storage.partitioner.Partitioner;
import io.confluent.connect.s3.S3SinkConnectorConfig;
import io.confluent.connect.s3.S3SinkTask;
import io.confluent.connect.s3.format.parquet.ParquetFormat;
import io.confluent.connect.s3.hive.HiveMetaStoreUpdaterImpl;
import io.confluent.connect.s3.hive.HiveTestBase;
import io.confluent.connect.s3.hive.HiveTestUtils;
import io.confluent.connect.s3.util.TimeUtils;
import io.confluent.connect.storage.common.StorageCommonConfig;
import java.util.Collection;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import io.confluent.connect.storage.hive.HiveConfig;
import io.confluent.connect.storage.partitioner.DailyPartitioner;
import io.confluent.connect.storage.partitioner.FieldPartitioner;
import io.confluent.connect.storage.partitioner.PartitionerConfig;
import java.util.Collections;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import static org.junit.Assert.assertEquals;

@RunWith(Parameterized.class)
public class HiveIntegrationParquetTest extends HiveTestBase<ParquetFormat> {

  private final Map<String, String> localProps = new HashMap<>();
  private final String hiveTableNameConfig;

  public HiveIntegrationParquetTest(final String hiveTableNameConfig) {
    super(ParquetFormat.class);
    this.hiveTableNameConfig = hiveTableNameConfig;
  }

  @Parameterized.Parameters(name = "{index}: hiveTableNameConfig={0}")
  public static Collection<Object[]> data() {
    return Arrays.asList(new Object[][] {
            { "${topic}" },
            { "a-${topic}-table" }
    });
  }

  @Before
  public void beforeTest() {
    localProps.put(S3SinkConnectorConfig.HIVE_TABLE_NAME_CONFIG, hiveTableNameConfig);
  }

  @Override
  protected Map<String, String> createProps() {
    Map<String, String> props = super.createProps();
    props.put(S3SinkConnectorConfig.SHUTDOWN_TIMEOUT_CONFIG, "10000");
    props.put(S3SinkConnectorConfig.FORMAT_CLASS_CONFIG, ParquetFormat.class.getName());
    props.putAll(localProps);
    return props;
  }

  @Test
  public void testHiveIntegrationParquet() throws Exception {
    localProps.put(HiveConfig.HIVE_INTEGRATION_CONFIG, "true");
    setUp();

    writeToS3(createRecords(7), partitioner);

    Schema schema = createSchema();
    String hiveTableName = connectorConfig.getHiveTableName(TOPIC);
    Table table = hiveMetaStore.getTable(hiveDatabase, hiveTableName);
    List<String> expectedColumnNames = new ArrayList<>();
    for (Field field : schema.fields()) {
      expectedColumnNames.add(field.name());
    }

    List<String> actualColumnNames = new ArrayList<>();
    for (FieldSchema column : table.getSd().getCols()) {
      actualColumnNames.add(column.getName());
    }
    assertEquals(expectedColumnNames, actualColumnNames);

    List<String> expectedPartitions = Arrays.asList(
      s3aFileKey(TOPIC, "partition=" + String.valueOf(PARTITION))
    );

    List<String> partitions = hiveMetaStore.listPartitions(hiveDatabase, hiveTableName, (short)-1);

    assertEquals(expectedPartitions, partitions);
  }

  @Test
  public void testHiveIntegrationFieldPartitionerParquet() throws Exception {
    int batchSize = 3;
    int batchNum = 3;
    localProps.put(HiveConfig.HIVE_INTEGRATION_CONFIG, "true");
    setUp();

    Schema schema = createSchema();
    List<Struct> records = createRecordBatches(schema, batchSize, batchNum);
    List<SinkRecord> sinkRecords = createSinkRecords(records, schema);

    Map<String, Object> fieldPartitionerConfig = new HashMap<>();
    fieldPartitionerConfig.put(PartitionerConfig.PARTITION_FIELD_NAME_CONFIG, Arrays.asList("int"));
    fieldPartitionerConfig.put(StorageCommonConfig.DIRECTORY_DELIM_CONFIG, "/");
    Partitioner fieldPartitioner = new FieldPartitioner();
    fieldPartitioner.configure(fieldPartitionerConfig);

    writeToS3(sinkRecords, fieldPartitioner);

    String hiveTableName = connectorConfig.getHiveTableName(TOPIC);
    Table table = hiveMetaStore.getTable(hiveDatabase, hiveTableName);

    List<String> expectedColumnNames = new ArrayList<>();
    for (Field field : schema.fields()) {
      expectedColumnNames.add(field.name());
    }
    Collections.sort(expectedColumnNames);

    List<String> actualColumnNames = new ArrayList<>();
    // getAllCols is needed to include columns used for partitioning in result
    for (FieldSchema column : table.getAllCols()) {
      actualColumnNames.add(column.getName());
    }
    Collections.sort(actualColumnNames);

    assertEquals(expectedColumnNames, actualColumnNames);

    List<String> partitionFieldNames = connectorConfig.getList(
        PartitionerConfig.PARTITION_FIELD_NAME_CONFIG
    );
    String partitionFieldName = partitionFieldNames.get(0);

    List<String> expectedPartitions = Arrays.asList(
      s3aFileKey(TOPIC, partitionFieldName + "=" + String.valueOf(16)),
      s3aFileKey(TOPIC, partitionFieldName + "=" + String.valueOf(17)),
      s3aFileKey(TOPIC, partitionFieldName + "=" + String.valueOf(18))
    );

    List<String> partitions = hiveMetaStore.listPartitions(hiveDatabase, hiveTableName, (short)-1);

    assertEquals(expectedPartitions, partitions);

    Struct sampleRecord = createRecord(schema, 16, 12.2f);
    List<List<String>> expectedResults = new ArrayList<>();
    for (int batch = 0; batch < batchNum; ++batch) {
      int intForBatch =  sampleRecord.getInt32("int") + batch;
      float floatForBatch =  sampleRecord.getFloat32("float") + (float) batch;
      double doubleForBatch = sampleRecord.getFloat64("double") + (double) batch;
      for (int row = 0; row < batchSize; ++row) {
        // the partition field as column is last
        List<String> result = new ArrayList<>(
            Arrays.asList("true", String.valueOf(intForBatch), String.valueOf(floatForBatch),
                String.valueOf(doubleForBatch), String.valueOf(intForBatch)));
        expectedResults.add(result);
      }
    }

    String result = HiveTestUtils.runHive(
        hiveExec,
        "SELECT * FROM " + hiveMetaStore.tableNameConverter(hiveTableName)
    );
    String[] rows = result.split("\n");
    assertEquals(batchNum * batchSize, rows.length);
    for (int i = 0; i < rows.length; ++i) {
      String[] parts = HiveTestUtils.parseOutput(rows[i]);
      int j = 0;
      for (String expectedValue : expectedResults.get(i)) {
        assertEquals(expectedValue, parts[j++]);
      }
    }
  }

  @Test
  public void testHiveIntegrationFieldPartitionerParquetMultiple() throws Exception {
    localProps.put(HiveConfig.HIVE_INTEGRATION_CONFIG, "true");
    setUp();

    Schema schema = SchemaBuilder.struct()
        .field("count", Schema.INT64_SCHEMA)
        .field("country", Schema.STRING_SCHEMA)
        .field("state", Schema.OPTIONAL_STRING_SCHEMA)
        .build();

    List<Struct> records = Arrays.asList(
        new Struct(schema)
            .put("count", 1L)
            .put("country", "us")
            .put("state", "tx"),
        new Struct(schema)
            .put("count", 1L)
            .put("country", "us")
            .put("state", "ca"),
        new Struct(schema)
            .put("count", 1L)
            .put("country", "mx")
            .put("state", null)
    );
    List<SinkRecord> sinkRecords = createSinkRecords(records, schema);

    Map<String, Object> fieldPartitionerConfig = new HashMap<>();
    fieldPartitionerConfig.put(PartitionerConfig.PARTITION_FIELD_NAME_CONFIG, Arrays.asList("country", "state"));
    fieldPartitionerConfig.put(StorageCommonConfig.DIRECTORY_DELIM_CONFIG, "/");
    Partitioner fieldPartitioner = new FieldPartitioner();
    fieldPartitioner.configure(fieldPartitionerConfig);

    writeToS3(sinkRecords, fieldPartitioner);

    String hiveTableName = connectorConfig.getHiveTableName(TOPIC);
    Table table = hiveMetaStore.getTable(hiveDatabase, hiveTableName);

    List<String> expectedColumnNames = new ArrayList<>();
    for (Field field : schema.fields()) {
      expectedColumnNames.add(field.name());
    }
    Collections.sort(expectedColumnNames);

    List<String> actualColumnNames = new ArrayList<>();
    // getAllCols is needed to include columns used for partitioning in result
    for (FieldSchema column : table.getAllCols()) {
      actualColumnNames.add(column.getName());
    }
    Collections.sort(actualColumnNames);

    assertEquals(expectedColumnNames, actualColumnNames);

    List<String> expectedPartitions = Arrays.asList(
      s3aFileKey(TOPIC, "country=mx/state=null"),
      s3aFileKey(TOPIC, "country=us/state=ca"),
      s3aFileKey(TOPIC, "country=us/state=tx")
    );

    List<String> partitions = hiveMetaStore.listPartitions(hiveDatabase, hiveTableName, (short)-1);

    assertEquals(expectedPartitions, partitions);

    List<List<String>> expectedResults = Arrays.asList(
        Arrays.asList("1", "mx", "null"),
        Arrays.asList("1", "us", "ca"),
        Arrays.asList("1", "us", "tx")
    );

    String result = HiveTestUtils.runHive(
        hiveExec,
        "SELECT * FROM " +
            hiveMetaStore.tableNameConverter(hiveTableName)
    );
    String[] rows = result.split("\n");
    assertEquals(expectedResults.size(), rows.length);
    for (int i = 0; i < rows.length; ++i) {
      String[] parts = HiveTestUtils.parseOutput(rows[i]);
      int j = 0;
      for (String expectedValue : expectedResults.get(i)) {
        assertEquals(expectedValue, parts[j++]);
      }
    }
  }

  @Test
  public void testHiveIntegrationTimeBasedPartitionerParquet() throws Exception {
    localProps.put(HiveConfig.HIVE_INTEGRATION_CONFIG, "true");
    setUp();

    Schema schema = createSchema();
    List<Struct> records = createRecordBatches(schema, 3, 3);
    List<SinkRecord> sinkRecords = createSinkRecords(records, schema);

    Map<String, Object> dailyPartitionerConfig = new HashMap<>();
    dailyPartitionerConfig.put(StorageCommonConfig.DIRECTORY_DELIM_CONFIG, "/");
    dailyPartitionerConfig.put(PartitionerConfig.TIMESTAMP_EXTRACTOR_CLASS_CONFIG, "Wallclock");
    dailyPartitionerConfig.put(PartitionerConfig.LOCALE_CONFIG, "America/Los_Angeles");
    dailyPartitionerConfig.put(PartitionerConfig.TIMEZONE_CONFIG, DateTimeZone.forID("America/Los_Angeles").getID());
    Partitioner dailyPartitioner = new DailyPartitioner();
    dailyPartitioner.configure(dailyPartitionerConfig);

    writeToS3(sinkRecords, dailyPartitioner);

    String hiveTableName = connectorConfig.getHiveTableName(TOPIC);
    Table table = hiveMetaStore.getTable(hiveDatabase, hiveTableName);

    List<String> expectedColumnNames = new ArrayList<>();
    for (Field field : schema.fields()) {
      expectedColumnNames.add(field.name());
    }

    List<String> actualColumnNames = new ArrayList<>();
    for (FieldSchema column : table.getSd().getCols()) {
      actualColumnNames.add(column.getName());
    }
    assertEquals(expectedColumnNames, actualColumnNames);

    String pathFormat = "'year'=YYYY/'month'=MM/'day'=dd";
    DateTime dateTime = DateTime.now(DateTimeZone.forID("America/Los_Angeles"));
    String encodedPartition = TimeUtils
        .encodeTimestamp(TimeUnit.HOURS.toMillis(24), pathFormat, "America/Los_Angeles",
                         dateTime.getMillis());

    List<String> expectedPartitions = Arrays.asList(
      s3aFileKey(TOPIC,encodedPartition)
    );

    List<String> partitions = hiveMetaStore.listPartitions(hiveDatabase, hiveTableName, (short)-1);
    assertEquals(expectedPartitions, partitions);

    ArrayList<String> partitionFields = new ArrayList<>();
    String[] groups = encodedPartition.split("/");
    for (String group : groups) {
      String field = group.split("=")[1];
      partitionFields.add(field);
    }

    List<List<String>> expectedResults = new ArrayList<>();
    for (int j = 0; j < 3; ++j) {
      for (int i = 0; i < 3; ++i) {
        List<String> result = Arrays.asList("true",
                                            String.valueOf(16 + i),
                                            String.valueOf((long) (16 + i)),
                                            String.valueOf(12.2f + i),
                                            String.valueOf((double) (12.2f + i)),
                                            partitionFields.get(0),
                                            partitionFields.get(1),
                                            partitionFields.get(2));
        expectedResults.add(result);
      }
    }

    String result = HiveTestUtils.runHive(
        hiveExec,
        "SELECT * FROM " + hiveMetaStore.tableNameConverter(hiveTableName)
    );
    String[] rows = result.split("\n");
    assertEquals(9, rows.length);
    for (int i = 0; i < rows.length; ++i) {
      String[] parts = HiveTestUtils.parseOutput(rows[i]);
      int j = 0;
      for (String expectedValue : expectedResults.get(i)) {
        assertEquals(expectedValue, parts[j++]);
      }
    }
  }
}
