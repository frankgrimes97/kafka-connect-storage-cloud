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

package io.confluent.connect.s3;

import com.amazonaws.SdkClientException;
import io.confluent.connect.s3.storage.S3Storage;
import io.confluent.connect.s3.util.RetryUtil;
import io.confluent.connect.storage.errors.PartitionException;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.errors.IllegalWorkerStateException;
import org.apache.kafka.connect.errors.SchemaProjectorException;
import org.apache.kafka.connect.sink.ErrantRecordReporter;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTaskContext;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.Optional;
import java.util.Queue;

import io.confluent.common.utils.SystemTime;
import io.confluent.common.utils.Time;
import io.confluent.connect.storage.StorageSinkConnectorConfig;
import io.confluent.connect.storage.common.StorageCommonConfig;
import io.confluent.connect.storage.common.util.StringUtils;
import io.confluent.connect.storage.format.RecordWriter;
import io.confluent.connect.storage.format.RecordWriterProvider;
import io.confluent.connect.storage.partitioner.Partitioner;
import io.confluent.connect.storage.partitioner.PartitionerConfig;
import io.confluent.connect.storage.partitioner.TimeBasedPartitioner;
import io.confluent.connect.storage.partitioner.TimestampExtractor;
import io.confluent.connect.storage.schema.StorageSchemaCompatibility;
import io.confluent.connect.storage.util.DateTimeUtils;

import static io.confluent.connect.s3.S3SinkConnectorConfig.S3_PART_RETRIES_CONFIG;
import static io.confluent.connect.s3.S3SinkConnectorConfig.S3_RETRY_BACKOFF_CONFIG;
import static io.confluent.connect.s3.S3SinkConnectorConfig.SEPARATE_FILE_PER_SCHEMA_VERSION_CONFIG;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.atomic.LongAdder;

public class TopicPartitionWriter {
  private static final Logger log = LoggerFactory.getLogger(TopicPartitionWriter.class);

  private final Map<String, String> commitFiles;
  private final Map<String, RecordWriter> writers;
  private final Map<String, Schema> currentSchemas;
  private final TopicPartition tp;
  private final S3Storage storage;
  private final Partitioner<?> partitioner;
  private final TimestampExtractor timestampExtractor;
  private String topicsDir;
  private State state;
  private final Queue<SinkRecord> buffer;
  private final SinkTaskContext context;
  private final boolean isTaggingEnabled;
  private final boolean ignoreTaggingErrors;
  private long recordCount;
  private final int flushSize;
  private final long rotateIntervalMs;
  private final long rotateScheduleIntervalMs;
  private long nextScheduledRotation;
  private long currentOffset;
  private final int maxOpenFilesPerPartition;
  private final boolean separateFilePerSchemaVersion;
  private Long currentTimestamp;
  private String currentEncodedPartition;
  private final Map<String, Long> currentTimestamps;
  private final Set<String> openEncodedPartitions;
  private final Map<String, Long> baseRecordTimestamps;
  private Long offsetToCommit;
  private final RecordWriterProvider<S3SinkConnectorConfig> writerProvider;
  private final Map<String, Long> startOffsets;
  private final Map<String, Long> endOffsets;
  private final Map<String, LongAdder> encodedPartitionRecordCounts;
  private long timeoutMs;
  private long failureTime;
  private final StorageSchemaCompatibility compatibility;
  private final String extension;
  private final String zeroPadOffsetFormat;
  private final String dirDelim;
  private final String fileDelim;
  private final Time time;
  private DateTimeZone timeZone;
  private final S3SinkConnectorConfig connectorConfig;
  private static final Time SYSTEM_TIME = new SystemTime();
  private ErrantRecordReporter reporter;

  public TopicPartitionWriter(TopicPartition tp,
                              S3Storage storage,
                              RecordWriterProvider<S3SinkConnectorConfig> writerProvider,
                              Partitioner<?> partitioner,
                              S3SinkConnectorConfig connectorConfig,
                              SinkTaskContext context,
                              ErrantRecordReporter reporter) {
    this(tp, storage, writerProvider, partitioner, connectorConfig, context, SYSTEM_TIME, reporter);
  }

  // Visible for testing
  TopicPartitionWriter(TopicPartition tp,
                       S3Storage storage,
                       RecordWriterProvider<S3SinkConnectorConfig> writerProvider,
                       Partitioner<?> partitioner,
                       S3SinkConnectorConfig connectorConfig,
                       SinkTaskContext context,
                       Time time,
                       ErrantRecordReporter reporter
  ) {
    this.connectorConfig = connectorConfig;
    this.time = time;
    this.tp = tp;
    this.storage = storage;
    this.context = context;
    this.writerProvider = writerProvider;
    this.partitioner = partitioner;
    this.reporter = reporter;
    this.timestampExtractor = partitioner instanceof TimeBasedPartitioner
                                  ? ((TimeBasedPartitioner) partitioner).getTimestampExtractor()
                                  : null;
    isTaggingEnabled = connectorConfig.getBoolean(S3SinkConnectorConfig.S3_OBJECT_TAGGING_CONFIG);
    ignoreTaggingErrors = connectorConfig.getString(
            S3SinkConnectorConfig.S3_OBJECT_BEHAVIOR_ON_TAGGING_ERROR_CONFIG)
            .equalsIgnoreCase(S3SinkConnectorConfig.IgnoreOrFailBehavior.IGNORE.toString());
    flushSize = connectorConfig.getInt(S3SinkConnectorConfig.FLUSH_SIZE_CONFIG);
    topicsDir = connectorConfig.getString(StorageCommonConfig.TOPICS_DIR_CONFIG);
    rotateIntervalMs = connectorConfig.getLong(S3SinkConnectorConfig.ROTATE_INTERVAL_MS_CONFIG);
    if (rotateIntervalMs > 0 && timestampExtractor == null) {
      log.warn(
          "Property '{}' is set to '{}ms' but partitioner is not an instance of '{}'. This property"
              + " is ignored.",
          S3SinkConnectorConfig.ROTATE_INTERVAL_MS_CONFIG,
          rotateIntervalMs,
          TimeBasedPartitioner.class.getName()
      );
    }
    rotateScheduleIntervalMs =
        connectorConfig.getLong(S3SinkConnectorConfig.ROTATE_SCHEDULE_INTERVAL_MS_CONFIG);
    if (rotateScheduleIntervalMs > 0) {
      timeZone = DateTimeZone.forID(connectorConfig.getString(PartitionerConfig.TIMEZONE_CONFIG));
    }
    timeoutMs = connectorConfig.getLong(S3SinkConnectorConfig.RETRY_BACKOFF_CONFIG);
    compatibility = StorageSchemaCompatibility.getCompatibility(
        connectorConfig.getString(StorageSinkConnectorConfig.SCHEMA_COMPATIBILITY_CONFIG));

    maxOpenFilesPerPartition = connectorConfig.getInt(
        S3SinkConnectorConfig.MAX_OPEN_FILES_PER_PARTITION_CONFIG);

    separateFilePerSchemaVersion = connectorConfig.getBoolean(
        S3SinkConnectorConfig.SEPARATE_FILE_PER_SCHEMA_VERSION_CONFIG);

    buffer = new LinkedList<>();
    commitFiles = new HashMap<>();
    writers = new HashMap<>();
    currentSchemas = new HashMap<>();
    currentTimestamps = new HashMap<>();
    openEncodedPartitions = new HashSet<>();
    baseRecordTimestamps = new HashMap<>();
    startOffsets = new HashMap<>();
    endOffsets = new HashMap<>();
    encodedPartitionRecordCounts = new HashMap<>();
    state = State.WRITE_STARTED;
    failureTime = -1L;
    currentOffset = -1L;
    dirDelim = connectorConfig.getString(StorageCommonConfig.DIRECTORY_DELIM_CONFIG);
    fileDelim = connectorConfig.getString(StorageCommonConfig.FILE_DELIM_CONFIG);
    extension = writerProvider.getExtension();
    zeroPadOffsetFormat = "%0"
        + connectorConfig.getInt(S3SinkConnectorConfig.FILENAME_OFFSET_ZERO_PAD_WIDTH_CONFIG)
        + "d";

    // Initialize scheduled rotation timer if applicable
    setNextScheduledRotation();
  }

  private enum State {
    WRITE_STARTED,
    WRITE_PARTITION_PAUSED,
    SHOULD_ROTATE,
    FILE_COMMITTED;

    private static final State[] VALS = values();

    public State next() {
      return VALS[(ordinal() + 1) % VALS.length];
    }
  }

  public void write() {
    long now = time.milliseconds();
    if (failureTime > 0 && now - failureTime < timeoutMs) {
      return;
    } else {
      failureTime = -1;
    }

    resetExpiredScheduledRotationIfNoPendingRecords(now);

    while (!buffer.isEmpty()) {
      try {
        executeState(now);
      } catch (IllegalWorkerStateException e) {
        throw new ConnectException(e);
      } catch (SchemaProjectorException e) {
        if (reporter != null) {
          reporter.report(buffer.poll(), e);
          log.warn("Errant record written to DLQ due to: {}", e.getMessage());
        } else {
          throw e;
        }
      }
    }
    commitOnTimeIfNoData(now);
  }

  @SuppressWarnings("fallthrough")
  private void executeState(long now) {
    switch (state) {
      case WRITE_STARTED:
        pause();
        nextState();
        // fallthrough
      case WRITE_PARTITION_PAUSED:
        SinkRecord record = buffer.peek();
        Schema valueSchema = record.valueSchema();
        String encodedPartition;
        try {
          encodedPartition = partitioner.encodePartition(record, now);
          if (separateFilePerSchemaVersion) {
            encodedPartition = encodedPartition + fileDelim + valueSchema.version();
          }
        } catch (PartitionException e) {
          if (reporter != null) {
            reporter.report(record, e);
            buffer.poll();
            break;
          } else {
            throw e;
          }
        }

        if (timestampExtractor != null) {
          final long currentRecordTimestamp = timestampExtractor.extract(record, now);
          currentTimestamp = currentRecordTimestamp;
          currentTimestamps.put(encodedPartition, currentRecordTimestamp);
          if (!baseRecordTimestamps.containsKey(encodedPartition)) {
            baseRecordTimestamps.put(encodedPartition, currentRecordTimestamp);
          }
        }

        Schema currentValueSchema = currentSchemas.get(encodedPartition);
        if (currentValueSchema == null) {
          currentSchemas.put(encodedPartition, valueSchema);
          currentValueSchema = valueSchema;
        }

        if (!checkRotationOrAppend(
            record,
            currentValueSchema,
            valueSchema,
            encodedPartition,
            now
        )) {
          break;
        }
        // fallthrough
      case SHOULD_ROTATE:
        commitFiles();
        nextState();
        // fallthrough
      case FILE_COMMITTED:
        setState(State.WRITE_PARTITION_PAUSED);
        break;
      default:
        log.error("{} is not a valid state to write record for topic partition {}.", state, tp);
    }
  }

  /**
   * Check if we should rotate the file (schema change, time-based).
   * @returns true if rotation is being performed, false otherwise
   */
  private boolean checkRotationOrAppend(
      SinkRecord record,
      Schema currentValueSchema,
      Schema valueSchema,
      String encodedPartition,
      long now
  ) {

    // rotateOnTime is safe to go before writeRecord, because it is acceptable
    // even for a faulty record to trigger time-based rotation if it applies
    if (rotateOnTime(
          encodedPartition,
          currentTimestamps.getOrDefault(encodedPartition, currentTimestamp),
          now)) {

      setNextScheduledRotation();
      nextState();
      return true;
    }

    if (compatibility.shouldChangeSchema(record, null, currentValueSchema)
        && recordCount > 0) {
      // This branch is never true for the first record read by this TopicPartitionWriter
      log.trace(
          "Incompatible change of schema detected for record '{}' with encoded partition "
          + "'{}' and current offset: '{}'",
          record,
          encodedPartition,
          currentOffset
      );
      currentSchemas.put(encodedPartition, valueSchema);
      nextState();
      return true;
    }

    SinkRecord projectedRecord = compatibility.project(record, null, currentValueSchema);
    boolean validRecord = writeRecord(projectedRecord, encodedPartition);
    buffer.poll();
    if (!validRecord) {
      // skip the faulty record and don't rotate
      return false;
    }

    if (rotateOnSize(encodedPartition)) {
      log.info(
          "Starting commit and rotation for encodedPartition {} with start offset {}",
          encodedPartition,
          startOffsets.getOrDefault(encodedPartition, 0L)
      );
      nextState();
      return true;
    }

    return false;
  }

  private void commitOnTimeIfNoData(long now) {
    if (buffer.isEmpty()) {
      boolean shouldCommitFiles = false;
      // committing files after waiting for rotateIntervalMs time but less than flush.size
      // records available
      if (maxOpenFilesPerPartition == 1
          && recordCount > 0
          && rotateOnTime(currentEncodedPartition, currentTimestamp, now)) {

        log.info(
            "Committing files after waiting for rotateIntervalMs time but less than flush.size "
            + "records available."
        );
        setNextScheduledRotation();

        shouldCommitFiles = true;
      } else if (maxOpenFilesPerPartition > 1) {
        for (String encodedPartition : openEncodedPartitions) {
          if (rotateOnTime(
                encodedPartition,
                currentTimestamps.getOrDefault(encodedPartition, now),
                now)) {

            log.info(
                "Committing files after waiting for rotateIntervalMs time for encodedPartition "
                + "'{}' but less than flush.size records available.",
                encodedPartition);

            setNextScheduledRotation();

            // At least one encodedPartition needs committing, so commit all so that
            // the S3SinkTask's preCommit contains no buffered offsets, only
            // those writen to S3
            shouldCommitFiles = true;
            break;
          }
        }
      }

      if (shouldCommitFiles) {
        commitFiles();
      }
      resume();
      setState(State.WRITE_STARTED);
    }
  }

  private void resetExpiredScheduledRotationIfNoPendingRecords(long now) {
    if (recordCount == 0 && shouldApplyScheduledRotation(now)) {
      setNextScheduledRotation();
    }
  }

  public void close() throws ConnectException {
    log.debug("Closing TopicPartitionWriter {}", tp);
    for (RecordWriter writer : writers.values()) {
      writer.close();
    }
    writers.clear();
    startOffsets.clear();
  }

  public void buffer(SinkRecord sinkRecord) {
    buffer.add(sinkRecord);
  }

  public Long getOffsetToCommitAndReset() {
    Long latest = offsetToCommit;
    offsetToCommit = null;
    return latest;
  }

  public Long currentStartOffset() {
    return minStartOffset();
  }

  public void failureTime(long when) {
    this.failureTime = when;
  }

  private Long minStartOffset() {
    Optional<Long> minStartOffset = startOffsets.values().stream()
        .min(Comparator.comparing(Long::valueOf));
    return minStartOffset.isPresent() ? minStartOffset.get() : null;
  }

  private String getDirectoryPrefix(String encodedPartition) {
    return partitioner.generatePartitionedPath(tp.topic(), encodedPartition);
  }

  private void nextState() {
    state = state.next();
  }

  private void setState(State state) {
    this.state = state;
  }

  private static final boolean recordTimestampExceedsRotationInterval(
      final Long recordTimestamp,
      final Long baseRecordTimestamp,
      final long rotateIntervalMs) {

    return recordTimestamp - baseRecordTimestamp >= rotateIntervalMs;
  }

  private static final boolean encodedPartitionChangeNecessitatesRotation(
      final String encodedPartition,
      final String currentEncodedPartition,
      final int maxOpenFilesPerPartition) {

    return !encodedPartition.equals(currentEncodedPartition)
          && maxOpenFilesPerPartition == 1;
  }

  private boolean rotateOnTime(String encodedPartition, Long recordTimestamp, long now) {
    if (recordCount <= 0) {
      return false;
    }

    final Long baseRecordTimestamp =
        baseRecordTimestamps.getOrDefault(encodedPartition, currentTimestamp);

    // rotateIntervalMs > 0 implies timestampExtractor != null
    final boolean hasTimestampExtractor = rotateIntervalMs > 0 && timestampExtractor != null;
    boolean periodicRotation = hasTimestampExtractor
        && (recordTimestampExceedsRotationInterval(
              recordTimestamp,
              baseRecordTimestamp,
              rotateIntervalMs)
            || encodedPartitionChangeNecessitatesRotation(
                 encodedPartition,
                 currentEncodedPartition,
                 maxOpenFilesPerPartition)
            || baseRecordTimestamps.size() > maxOpenFilesPerPartition
        );

    log.trace(
        "Checking rotation on time for topic-partition '{}' "
            + "with recordCount '{}' and encodedPartition '{}'",
        tp,
        maxOpenFilesPerPartition == 1
          ? recordCount
          : encodedPartitionRecordCounts.getOrDefault(
              encodedPartition,
              new LongAdder()
            ).longValue(),
        encodedPartition
    );

    log.trace(
        "Should apply periodic time-based rotation for topic-partition '{}':"
            + " (rotateIntervalMs: '{}', baseRecordTimestamp: "
            + "'{}', timestamp: '{}', encodedPartition: '{}', currentEncodedPartition: '{}')? {}",
        tp,
        rotateIntervalMs,
        baseRecordTimestamp,
        recordTimestamp,
        encodedPartition,
        currentEncodedPartition,
        periodicRotation
    );
    return periodicRotation || shouldApplyScheduledRotation(now);
  }

  private boolean shouldApplyScheduledRotation(long now) {
    boolean scheduledRotation = rotateScheduleIntervalMs > 0 && now >= nextScheduledRotation;
    log.trace(
        "Should apply scheduled rotation for topic-partition '{}':"
            + " (rotateScheduleIntervalMs: '{}', nextScheduledRotation:"
            + " '{}', now: '{}')? {}",
        tp,
        rotateScheduleIntervalMs,
        nextScheduledRotation,
        now,
        scheduledRotation
    );
    return scheduledRotation;
  }

  private void setNextScheduledRotation() {
    if (rotateScheduleIntervalMs > 0) {
      long now = time.milliseconds();
      nextScheduledRotation = DateTimeUtils.getNextTimeAdjustedByDay(
          now,
          rotateScheduleIntervalMs,
          timeZone
      );
      if (log.isDebugEnabled()) {
        log.debug(
            "Update scheduled rotation timer for topic-partition '{}': "
                + "(rotateScheduleIntervalMs: '{}', nextScheduledRotation: '{}', now: '{}'). "
                + "Next rotation will be at {}",
            tp,
            rotateScheduleIntervalMs,
            nextScheduledRotation,
            now,
            new DateTime(nextScheduledRotation).withZone(timeZone).toString()
        );
      }
    }
  }

  private static final long getRecordCount(
      final String encodedPartition,
      final Map<String, LongAdder> recordCountsPerEncodedPartition) {

    return recordCountsPerEncodedPartition
        .getOrDefault(encodedPartition, new LongAdder())
        .longValue();
  }

  private boolean rotateOnSize(final String encodedPartition) {
    long encodedPartitionRecordCount = maxOpenFilesPerPartition == 1 ? recordCount : getRecordCount(
        encodedPartition,
        encodedPartitionRecordCounts
    );
    boolean messageSizeRotation = encodedPartitionRecordCount >= flushSize;
    log.trace("Should apply size-based rotation for topic-partition '{}':"
            + " (count {} >= flush size {})? {}",
        tp,
        encodedPartitionRecordCount,
        flushSize,
        messageSizeRotation
    );
    return messageSizeRotation;
  }

  private void pause() {
    log.trace("Pausing writer for topic-partition '{}'", tp);
    context.pause(tp);
  }

  private void resume() {
    log.trace("Resuming writer for topic-partition '{}'", tp);
    context.resume(tp);
  }

  private RecordWriter newWriter(SinkRecord record, String encodedPartition)
      throws ConnectException {
    String commitFilename = getCommitFilename(encodedPartition);
    log.debug(
        "Creating new writer encodedPartition='{}' filename='{}'",
        encodedPartition,
        commitFilename
    );
    RecordWriter writer = writerProvider.getRecordWriter(connectorConfig, commitFilename);
    writers.put(encodedPartition, writer);
    return writer;
  }

  private String getCommitFilename(String encodedPartition) {
    String commitFile;
    if (commitFiles.containsKey(encodedPartition)) {
      commitFile = commitFiles.get(encodedPartition);
    } else {
      String tmp = encodedPartition;
      String schemaVersionSuffix = "";
      if (separateFilePerSchemaVersion) {
        tmp = encodedPartition.substring(0, encodedPartition.lastIndexOf(fileDelim));
        schemaVersionSuffix = encodedPartition.substring(encodedPartition.lastIndexOf(fileDelim));
      }
      long startOffset = startOffsets.get(encodedPartition);
      String prefix = getDirectoryPrefix(tmp);
      commitFile = fileKeyToCommit(prefix, startOffset, schemaVersionSuffix);
      commitFiles.put(encodedPartition, commitFile);
    }
    return commitFile;
  }

  private String fileKey(String topicsPrefix, String keyPrefix, String name) {
    String suffix = keyPrefix + dirDelim + name;
    return StringUtils.isNotBlank(topicsPrefix)
           ? topicsPrefix + dirDelim + suffix
           : suffix;
  }

  private String fileKeyToCommit(String dirPrefix, long startOffset, String schemaVersionSuffix) {
    String name = tp.topic()
                      + fileDelim
                      + tp.partition()
                      + fileDelim
                      + String.format(zeroPadOffsetFormat, startOffset)
                      + schemaVersionSuffix
                      + extension;
    return fileKey(topicsDir, dirPrefix, name);
  }

  private boolean writeRecord(SinkRecord record, String encodedPartition) {
    RecordWriter writer = writers.get(encodedPartition);
    long currentOffsetIfSuccessful = record.kafkaOffset();
    boolean shouldRemoveWriter = false;
    boolean shouldRemoveStartOffset = false;
    boolean shouldRemoveCommitFilename = false;
    try {
      if (!startOffsets.containsKey(encodedPartition)) {
        log.trace(
            "Setting writer's start offset for '{}' to {}",
            encodedPartition,
            currentOffsetIfSuccessful
        );
        startOffsets.put(encodedPartition, currentOffsetIfSuccessful);
        shouldRemoveStartOffset = true;
      }
      if (writer == null) {
        if (!commitFiles.containsKey(encodedPartition)) {
          shouldRemoveCommitFilename = true;
        }
        writer = newWriter(record, encodedPartition);
        shouldRemoveWriter = true;
      }
      writer.write(record);
    } catch (DataException e) {
      if (reporter != null) {
        if (shouldRemoveStartOffset) {
          startOffsets.remove(encodedPartition);
        }
        if (shouldRemoveWriter) {
          writers.remove(encodedPartition);
        }
        if (shouldRemoveCommitFilename) {
          commitFiles.remove(encodedPartition);
        }
        reporter.report(record, e);
        log.warn("Errant record written to DLQ due to: {}", e.getMessage());
        return false;
      } else {
        throw new ConnectException(e);
      }
    }

    currentEncodedPartition = encodedPartition;
    openEncodedPartitions.add(encodedPartition);
    currentOffset = record.kafkaOffset();
    if (shouldRemoveStartOffset) {
      log.trace(
          "Setting writer's current offset for '{}' to {}",
          currentEncodedPartition,
          currentOffset
      );

      // Once we have a "start offset" for a particular "encoded partition"
      // value, we know that we have at least one record. This allows us
      // to initialize all our maps at the same time, and saves future
      // checks on the existence of keys
      encodedPartitionRecordCounts.remove(currentEncodedPartition);
      endOffsets.remove(currentEncodedPartition);
    }
    ++recordCount;

    encodedPartitionRecordCounts.computeIfAbsent(
      currentEncodedPartition,
      k -> new LongAdder()
    ).increment();
    endOffsets.put(currentEncodedPartition, currentOffset);
    return true;
  }

  private void commitFiles() {
    for (Map.Entry<String, String> entry : commitFiles.entrySet()) {
      String encodedPartition = entry.getKey();
      commitFile(encodedPartition);
      if (isTaggingEnabled) {
        RetryUtil.exponentialBackoffRetry(() -> tagFile(encodedPartition, entry.getValue()),
                ConnectException.class,
                connectorConfig.getInt(S3_PART_RETRIES_CONFIG),
                connectorConfig.getLong(S3_RETRY_BACKOFF_CONFIG)
        );
      }
      startOffsets.remove(encodedPartition);
      endOffsets.remove(encodedPartition);
      encodedPartitionRecordCounts.remove(encodedPartition);
      log.debug("Committed {} for {}", entry.getValue(), tp);
      baseRecordTimestamps.remove(encodedPartition);
    }

    offsetToCommit = currentOffset + 1;
    commitFiles.clear();
    currentSchemas.clear();
    recordCount = 0;
    log.info("Files committed to S3. Target commit offset for {} is {}", tp, offsetToCommit);
  }

  private void commitFile(String encodedPartition) {
    if (!startOffsets.containsKey(encodedPartition)) {
      log.warn("Tried to commit file with missing starting offset partition: {}. Ignoring.");
      return;
    }

    if (writers.containsKey(encodedPartition)) {
      RecordWriter writer = writers.get(encodedPartition);
      // Commits the file and closes the underlying output stream.
      writer.commit();
      writers.remove(encodedPartition);
      log.debug("Removed writer for '{}'", encodedPartition);
    }
  }

  private void tagFile(String encodedPartition, String s3ObjectPath) {
    Long startOffset = startOffsets.get(encodedPartition);
    Long endOffset = endOffsets.get(encodedPartition);
    Long recordCount = getRecordCount(encodedPartition, encodedPartitionRecordCounts);
    if (startOffset == null || endOffset == null || recordCount == 0L) {
      log.warn(
          "Missing tags when attempting to tag file {}. "
              + "Starting offset tag: {}, "
              + "ending offset tag: {}, "
              + "record count tag: {}. Ignoring.",
          encodedPartition,
          startOffset == null ? "missing" : startOffset,
          endOffset == null ? "missing" : endOffset,
          recordCount == 0L ? "missing" : recordCount
      );
      return;
    }

    log.debug("Object to tag is: {}", s3ObjectPath);
    Map<String, String> tags = new HashMap<>();
    tags.put("startOffset", Long.toString(startOffset));
    tags.put("endOffset", Long.toString(endOffset));
    tags.put("recordCount", Long.toString(recordCount));

    try {
      storage.addTags(s3ObjectPath, tags);
      log.info("Tagged S3 object {} with starting offset {}, ending offset {}, record count {}",
          s3ObjectPath, startOffset, endOffset, recordCount);
    } catch (SdkClientException e) {
      if (ignoreTaggingErrors) {
        log.warn("Unable to tag S3 object {}. Ignoring.", s3ObjectPath, e);
      } else {
        throw new ConnectException(String.format("Unable to tag S3 object %s", s3ObjectPath), e);
      }
    } catch (Exception e) {
      if (ignoreTaggingErrors) {
        log.warn("Unrecoverable exception while attempting to tag S3 object {}. Ignoring.",
                s3ObjectPath, e);
      } else {
        throw new ConnectException(String.format("Unable to tag S3 object %s", s3ObjectPath), e);
      }
    }
  }
}
