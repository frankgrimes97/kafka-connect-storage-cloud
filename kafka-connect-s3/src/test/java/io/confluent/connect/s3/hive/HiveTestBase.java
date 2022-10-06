/*
 * Copyright 2022 Confluent Inc.
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

package io.confluent.connect.s3.hive;

import com.amazonaws.services.s3.AmazonS3;
import io.confluent.connect.s3.S3SinkConnectorConfig;
import io.confluent.connect.s3.TestWithMockedS3;
import io.confluent.connect.s3.storage.S3Storage;
import io.confluent.connect.storage.common.StorageCommonConfig;
import io.confluent.connect.storage.common.util.StringUtils;
import io.confluent.connect.storage.format.Format;
import org.junit.After;

import java.util.Map;

import io.confluent.connect.storage.hive.HiveConfig;
import io.confluent.connect.storage.hive.HiveMetaStore;
import io.confluent.connect.storage.partitioner.DefaultPartitioner;
import io.confluent.connect.storage.partitioner.Partitioner;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.StringJoiner;
import java.util.UUID;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.sink.SinkRecord;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import org.junit.Rule;
import org.junit.rules.TemporaryFolder;
import org.powermock.api.mockito.PowerMockito;

public class HiveTestBase<
    FORMAT extends Format<S3SinkConnectorConfig, String>
  > extends TestWithMockedS3 {

  protected S3Storage storage;
  protected AmazonS3 s3;
  protected Partitioner<?> partitioner;
  protected FORMAT format;
  private final Class<FORMAT> clazz;

  protected String hiveDatabase;
  protected HiveMetaStore hiveMetaStore;
  protected HiveExec hiveExec;

  @Rule
  public TemporaryFolder hiveMetaStoreWarehouseDir = new TemporaryFolder();

  /**
   * Constructor
   *
   * @param clazz  A Class<Format type> object for the purpose of creating
   *               a new format object.
   */
  protected HiveTestBase(final Class<FORMAT> clazz) {
    this.clazz = clazz;
  }

  @Override
  protected Map<String, String> createProps() {
    Map<String, String> props = super.createProps();
    props.put(S3SinkConnectorConfig.HIVE_S3_PROTOCOL_CONFIG, "s3a");
    props.put(S3SinkConnectorConfig.S3_PATH_STYLE_ACCESS_ENABLED_CONFIG, "true");
    props.put(StorageCommonConfig.DIRECTORY_DELIM_CONFIG, "/");
    props.put(StorageCommonConfig.FILE_DELIM_CONFIG, "/");
    return props;
  }

  //@Before should be omitted in order to be able to add properties per test.
  public void setUp() throws Exception {
    super.setUp();
    s3 = PowerMockito.spy(newS3Client(connectorConfig));
    storage = new S3Storage(connectorConfig, url, S3_TEST_BUCKET_NAME, s3);

    partitioner = new DefaultPartitioner<>();
    partitioner.configure(parsedConfig);
    format = clazz.getDeclaredConstructor(S3Storage.class).newInstance(storage);
    assertEquals(format.getClass().getName(), clazz.getName());

    s3.createBucket(S3_TEST_BUCKET_NAME);
    assertTrue(s3.doesBucketExistV2(S3_TEST_BUCKET_NAME));

    hiveDatabase = connectorConfig.getString(HiveConfig.HIVE_DATABASE_CONFIG);

    Configuration hadoopConf = new Configuration();
    hadoopConf.set("fs.s3a.endpoint", url);
    hadoopConf.set(
      "fs.s3a.aws.credentials.provider",
      "org.apache.hadoop.fs.s3a.AnonymousAWSCredentialsProvider");
    hadoopConf.set("fs.s3a.path.style.access", "true");
    hadoopConf.set("fs.s3a.connection.ssl.enabled", "false");

    HiveConf hiveConf = new HiveConf(hadoopConf, HiveConf.class);
    hiveConf.set(
      "hive.metastore.warehouse.dir",
      hiveMetaStoreWarehouseDir.newFolder(
        "hive-tests-" +
        UUID.randomUUID().toString()
      ).getPath()
    );
    hiveConf.set("hive.metastore.schema.verification", "false");
    hiveConf.set("datanucleus.schema.autoCreateAll", "true");

    hiveMetaStore = new HiveMetaStore(hiveConf, connectorConfig);
    hiveExec = new HiveExec(connectorConfig);
    cleanHive();
  }

  @After
  public void tearDown() throws Exception {
    cleanHive();
    super.tearDown();
  }

  private void cleanHive() {
    // ensures all tables are removed
    for (String database : hiveMetaStore.getAllDatabases()) {
      for (String table : hiveMetaStore.getAllTables(database)) {
        hiveMetaStore.dropTable(database, table);
      }
      if (!"default".equals(database)) {
        hiveMetaStore.dropDatabase(database, false);
      }
    }
  }

  /**
   * Return a list of new records starting at zero offset.
   *
   * @param size the number of records to return.
   * @return the list of records.
   */
  protected List<SinkRecord> createRecords(int size) {
    return createRecords(size, 0);
  }

  /**
   * Return a list of new records starting at the given offset.
   *
   * @param size the number of records to return.
   * @param startOffset the starting offset.
   * @return the list of records.
   */
  protected List<SinkRecord> createRecords(int size, long startOffset) {
    return createRecords(size, startOffset, Collections.singleton(new TopicPartition(TOPIC, PARTITION)));
  }

  protected List<SinkRecord> createRecords(int size, long startOffset, Set<TopicPartition> partitions) {
    String key = "key";
    Schema schema = createSchema();
    Struct record = createRecord(schema);

    List<SinkRecord> sinkRecords = new ArrayList<>();
    for (TopicPartition tp : partitions) {
      for (long offset = startOffset; offset < startOffset + size; ++offset) {
        sinkRecords.add(new SinkRecord(TOPIC, tp.partition(), Schema.STRING_SCHEMA, key, schema, record, offset));
      }
    }
    return sinkRecords;
  }

  protected String s3aFileKey(
      String protocol,
      String bucketName,
      String topicsPrefix,
      String keyPrefix,
      String name) {

    StringJoiner sj = new StringJoiner("/")
      .add(protocol + ":/")
      .add(bucketName);

    if (StringUtils.isNotBlank(topicsPrefix)) {
      sj.add(topicsPrefix);
    }

    sj.add(keyPrefix)
      .add(name);

    return sj.toString();
  }

}
