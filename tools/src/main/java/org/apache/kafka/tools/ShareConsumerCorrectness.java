/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.kafka.tools;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaShareConsumer;
import org.apache.kafka.clients.consumer.ShareConsumer;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.Metric;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.TopicIdPartition;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.utils.Exit;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.server.util.CommandDefaultOptions;
import org.apache.kafka.server.util.CommandLineUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;

import joptsimple.OptionException;
import joptsimple.OptionSpec;

import static joptsimple.util.RegexMatcher.regex;
import static org.apache.kafka.server.util.CommandLineUtils.parseKeyValueArgs;

public class ShareConsumerCorrectness {
    private static final Logger LOG = LoggerFactory.getLogger(ShareConsumerCorrectness.class);

    public static void main(String[] args) {
        run(args, KafkaShareConsumer::new);
    }

    static void run(String[] args, Function<Properties, ShareConsumer<byte[], byte[]>> shareConsumerCreator) {
        try {
            LOG.info("Starting share consumer/consumers for measuring correctness...");
            ShareConsumerPerfOptions options = new ShareConsumerPerfOptions(args);
            AtomicLong totalRecordsRead = new AtomicLong(0);
            AtomicLong totalBytesRead = new AtomicLong(0);
            ConcurrentHashMap<ConsumptionRecordKey, List<ConsumptionRecordValue>> consumptionDetails = new ConcurrentHashMap<>();
            ConcurrentHashMap<TopicPartition, TreeSet<Long>> offsetDetails = new ConcurrentHashMap<>();

            List<ShareConsumer<byte[], byte[]>> shareConsumers = new ArrayList<>();
            for (int i = 0; i < options.threads(); i++) {
                shareConsumers.add(shareConsumerCreator.apply(options.props()));
            }
            long startMs = System.currentTimeMillis();
            consume(shareConsumers, options, totalRecordsRead, totalBytesRead, startMs, consumptionDetails, offsetDetails);

            List<Map<MetricName, ? extends Metric>> shareConsumersMetrics = new ArrayList<>();
            if (options.printMetrics()) {
                shareConsumers.forEach(shareConsumer -> shareConsumersMetrics.add(shareConsumer.metrics()));
            }
            shareConsumers.forEach(shareConsumer -> {
                @SuppressWarnings("UnusedLocalVariable")
                Map<TopicIdPartition, Optional<KafkaException>> ignored = shareConsumer.commitSync();
            });

            shareConsumersMetrics.forEach(ToolsUtils::printMetrics);
            shareConsumers.forEach(shareConsumer -> shareConsumer.close(Duration.ofMillis(500)));
        } catch (Throwable e) {
            System.err.println(e.getMessage());
            System.err.println(Utils.stackTrace(e));
            Exit.exit(1);
        }
    }

    protected static String printStatsStr() {
        return "start.time = %s, " + "end.time = %s, " + "data.consumed.in.MB = %.4f, " + "MB/sec = %.4f, " +
            "nMsg/sec = %.4f, " + "data.consumed.in.nMsg = %d, " + "fetch.time.ms = %d\n";
    }

    private static void consume(
        List<ShareConsumer<byte[], byte[]>> shareConsumers,
        ShareConsumerPerfOptions options,
        AtomicLong totalRecordsRead,
        AtomicLong totalBytesRead,
        long startMs,
        ConcurrentHashMap<ConsumptionRecordKey, List<ConsumptionRecordValue>> consumptionDetails,
        ConcurrentHashMap<TopicPartition, TreeSet<Long>> offsetDetails
    ) throws ExecutionException, InterruptedException {
        shareConsumers.forEach(shareConsumer -> shareConsumer.subscribe(options.topic()));

        // Now start the benchmark.
        AtomicLong recordsRead = new AtomicLong(0);
        AtomicLong bytesRead = new AtomicLong(0);
        AtomicLong lastTotalBytesRead = new AtomicLong(0);
        AtomicLong lastTotalRecordsRead = new AtomicLong(0);
        AtomicLong lastGroupRecordedTimeMs = new AtomicLong(System.currentTimeMillis());

        ExecutorService executorService = Executors.newFixedThreadPool(shareConsumers.size());
        List<Future<?>> futures = new ArrayList<>();
        for (int i = 0; i < shareConsumers.size(); i++) {
            final int index = i;
            futures.add(executorService.submit(() -> {
                try {
                    consumeRecordsForSingleShareConsumer(shareConsumers.get(index), recordsRead, bytesRead,
                        lastTotalRecordsRead, lastTotalBytesRead, startMs, lastGroupRecordedTimeMs, options,
                        consumptionDetails, offsetDetails, index + 1);
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }));
        }
        for (Future<?> future : futures) {
            future.get();
        }
        totalRecordsRead.set(recordsRead.get());
        totalBytesRead.set(bytesRead.get());
    }

    private static void consumeRecordsForSingleShareConsumer(
        ShareConsumer<byte[], byte[]> shareConsumer,
        AtomicLong totalRecordsRead,
        AtomicLong totalBytesRead,
        AtomicLong lastTotalRecordsRead,
        AtomicLong lastTotalBytesRead,
        long startMs,
        AtomicLong lastGroupRecordedTimeMs,
        ShareConsumerPerfOptions options,
        ConcurrentHashMap<ConsumptionRecordKey, List<ConsumptionRecordValue>> consumptionDetails,
        ConcurrentHashMap<TopicPartition, TreeSet<Long>> offsetDetails,
        int index
    ) throws InterruptedException {
        SimpleDateFormat dateFormat = options.dateFormat();
        long currentTimeMs = System.currentTimeMillis();
        long lastReportTimeMs = currentTimeMs;

        long lastBytesRead = 0L;
        long lastRecordsRead = 0L;
        long recordsReadByConsumer = 0L;
        long bytesReadByConsumer = 0L;
        while (true) {
            ConsumerRecords<byte[], byte[]> records = shareConsumer.poll(Duration.ofMillis(100));
            currentTimeMs = System.currentTimeMillis();
            for (ConsumerRecord<byte[], byte[]> record : records) {
                String topic = record.topic();
                int partition = record.partition();
                long offset = record.offset();
                int deliveryCount = -1;
                if (record.deliveryCount().isPresent())
                    deliveryCount = record.deliveryCount().get();
                // Add the consumed record to the map.
                addRecordInShareConsumption(
                    new ConsumptionRecordKey(topic, partition, offset),
                    new ConsumptionRecordValue(index, currentTimeMs, deliveryCount),
                    consumptionDetails
                );
                // Add the offsets related info.
                addOffsetsDetails(
                    new TopicPartition(topic, partition),
                    offset,
                    offsetDetails
                );

                recordsReadByConsumer += 1;
                totalRecordsRead.addAndGet(1);
                if (record.key() != null) {
                    bytesReadByConsumer += record.key().length;
                    totalBytesRead.addAndGet(record.key().length);
                }
                if (record.value() != null) {
                    bytesReadByConsumer += record.value().length;
                    totalBytesRead.addAndGet(record.value().length);
                }
            }
            // Reporting progress of consumers
            if (currentTimeMs - lastReportTimeMs >= options.reportingIntervalMs()) {
                // Print share consumer progress in reportingIntervalMs duration
                printShareConsumptionProgress(bytesReadByConsumer, lastBytesRead, recordsReadByConsumer, lastRecordsRead,
                    lastReportTimeMs, currentTimeMs, dateFormat, index, options.reportingIntervalMs());
                // Print overall consumer progress
                printStatsForOverallConsumption(bytesReadByConsumer, recordsReadByConsumer, startMs, currentTimeMs,
                    options.dateFormat(), index);

                lastReportTimeMs = currentTimeMs;
                lastRecordsRead = recordsReadByConsumer;
                lastBytesRead = bytesReadByConsumer;
            }

            // Reporting progress of group
            if (currentTimeMs - lastGroupRecordedTimeMs.get() >= options.reportingIntervalMs() && index == 1) {
                // Print share group progress in reportingIntervalMs duration
                printShareConsumptionProgress(totalBytesRead.get(), lastTotalBytesRead.get(), totalRecordsRead.get(),
                    lastTotalRecordsRead.get(), lastGroupRecordedTimeMs.get(), currentTimeMs, dateFormat, -1,
                    options.reportingIntervalMs());
                // Print share group overall progress
                printStatsForOverallConsumption(totalBytesRead.get(), totalRecordsRead.get(), startMs,
                    currentTimeMs, options.dateFormat(), -1);
                // Print completeness of offsets
                printCompletenessOfConsumedRecords(offsetDetails, 2000L);

                lastGroupRecordedTimeMs.set(currentTimeMs);
                lastTotalBytesRead.set(totalBytesRead.get());
                lastTotalRecordsRead.set(totalRecordsRead.get());
            }
        }
    }

    /**
     * This function adds the records to the consumption concurrent hashmap. Also prints out any duplicates if found.
     * @param key - The record consumption key
     * @param value - The record consumption value
     * @param consumptionDetails - The concurrent hashmap to which the record needs to be added
     */
    synchronized static void addRecordInShareConsumption(
        ConsumptionRecordKey key,
        ConsumptionRecordValue value,
        ConcurrentHashMap<ConsumptionRecordKey, List<ConsumptionRecordValue>> consumptionDetails
    ) {
        if (!consumptionDetails.containsKey(key)) {
            consumptionDetails.put(key, new ArrayList<>());
            consumptionDetails.get(key).add(value);
            return;
        }
        List<ConsumptionRecordValue> duplicates = consumptionDetails.get(key);
        StringBuilder duplicatePrintStr = new StringBuilder("Duplicate consumption found for " + key.toString());
        for (ConsumptionRecordValue duplicate : duplicates) {
            duplicatePrintStr.append(" ").append(duplicate.toString());
        }
        consumptionDetails.get(key).add(value);
        System.out.println("******** DUPLICATE RECORD INFO ********");
        System.out.println(duplicatePrintStr);
    }

    synchronized static void addOffsetsDetails(
        TopicPartition topicPartition,
        long newOffset,
        ConcurrentHashMap<TopicPartition, TreeSet<Long>> offsetDetails
    ) {
        if (!offsetDetails.containsKey(topicPartition)) {
            offsetDetails.put(topicPartition, new TreeSet<>());
            offsetDetails.get(topicPartition).add(newOffset);
            return;
        }
        offsetDetails.get(topicPartition).add(newOffset);
    }

    protected static void printShareConsumptionProgress(
        long bytesRead,
        long lastBytesRead,
        long recordsRead,
        long lastRecordsRead,
        long startMs,
        long endMs,
        SimpleDateFormat dateFormat,
        int index,
        long reportingIntervalMs
    ) {
        double elapsedMs = endMs - startMs;
        double intervalMbRead = ((bytesRead - lastBytesRead) * 1.0) / (1024 * 1024);
        double intervalMbPerSec = 1000.0 * intervalMbRead / elapsedMs;
        double intervalRecordsPerSec = ((recordsRead - lastRecordsRead) / elapsedMs) * 1000.0;
        long fetchTimeMs = endMs - startMs;

        if (index != -1) {
            System.out.printf("Share consumer %d consumption in %d ms metrics -> " + printStatsStr(), index,
                reportingIntervalMs, dateFormat.format(startMs), dateFormat.format(endMs), intervalMbRead,
                intervalMbPerSec, intervalRecordsPerSec, recordsRead - lastRecordsRead, fetchTimeMs);
        } else {
            System.out.printf("Share group consumption in %d ms metrics -> " + printStatsStr(), reportingIntervalMs,
                dateFormat.format(startMs), dateFormat.format(endMs), intervalMbRead, intervalMbPerSec,
                intervalRecordsPerSec, recordsRead - lastRecordsRead, fetchTimeMs);
        }
    }

    private static void printStatsForOverallConsumption(long bytesRead,
                                                        long recordsRead,
                                                        long startMs,
                                                        long endMs,
                                                        SimpleDateFormat dateFormat,
                                                        int index) {
        double totalMbRead = (bytesRead * 1.0) / (1024 * 1024);
        double elapsedSec = (endMs - startMs) / 1_000.0;
        long fetchTimeInMs = endMs - startMs;
        if (index != -1) {
            System.out.printf("OVERALL SHARE CONSUMER %d CONSUMPTION STATS -> " + printStatsStr(), index,
                dateFormat.format(startMs), dateFormat.format(endMs), totalMbRead, totalMbRead / elapsedSec,
                recordsRead / elapsedSec, recordsRead, fetchTimeInMs);
        }
        else {
            System.out.printf("OVERALL SHARE GROUP CONSUMPTION STATS -> " + printStatsStr(), dateFormat.format(startMs),
                dateFormat.format(endMs), totalMbRead, totalMbRead / elapsedSec, recordsRead / elapsedSec,
                recordsRead, fetchTimeInMs);
        }
    }

    private static void printCompletenessOfConsumedRecords(
        ConcurrentHashMap<TopicPartition, TreeSet<Long>> offsetDetails,
        long offsetsLimit
    ) {
        HashMap<TopicPartition, List<Long>> missingOffsetsByTopicPartition = missingOffsetsInConsumedRecords(
            offsetDetails, offsetsLimit);

        if (missingOffsetsByTopicPartition.isEmpty()) {
            return;
        }
        System.out.println("******** MISSING RECORDS INFO ********");
        for (Map.Entry<TopicPartition, List<Long>> entry : missingOffsetsByTopicPartition.entrySet()) {
            TopicPartition topicPartition = entry.getKey();
            List<Long> offsets = entry.getValue();
            StringBuilder offsetStr = new StringBuilder();
            for (Long offset : offsets) {
                offsetStr.append(offset.toString()).append(", ");
            }
            System.out.printf("Missing offsets %s in topic %s partition %d%n",
                offsetStr.substring(0, offsetStr.toString().length() - 2),
                topicPartition.topic(),
                topicPartition.partition());
        }
    }

    // Visible for testing
    static HashMap<TopicPartition, List<Long>> missingOffsetsInConsumedRecords(
        ConcurrentHashMap<TopicPartition, TreeSet<Long>> offsetDetails,
        long offsetsLimit
    ) {
        HashMap<TopicPartition, List<Long>> missingOffsetsByTopicPartition = new HashMap<>();
        for (Map.Entry<TopicPartition, TreeSet<Long>> entry : offsetDetails.entrySet()) {
            TopicPartition topicPartition = entry.getKey();
            TreeSet<Long> records = entry.getValue();
            if (records.isEmpty()) {
                continue;
            }
            long checkOffsetUpto = Math.max(records.last() - offsetsLimit, 0L);
            Iterator<Long> recordIterator = records.iterator();
            long previousOffset = recordIterator.next();

            while (recordIterator.hasNext()) {
                long currentOffset = recordIterator.next();
                if (currentOffset > checkOffsetUpto) {
                    break;
                }
                if (currentOffset - previousOffset != 1) {
                    long missingOffset = previousOffset + 1;
                    if (!missingOffsetsByTopicPartition.containsKey(topicPartition)) {
                        missingOffsetsByTopicPartition.put(topicPartition, new ArrayList<>());
                    }
                    while (missingOffset < currentOffset) {
                        missingOffsetsByTopicPartition.get(topicPartition).add(missingOffset);
                        missingOffset += 1;
                    }
                }
                previousOffset = currentOffset;
            }
        }

        return missingOffsetsByTopicPartition;
    }

    protected static class ShareConsumerPerfOptions extends CommandDefaultOptions {
        private final OptionSpec<String> bootstrapServerOpt;
        private final OptionSpec<String> topicOpt;
        private final OptionSpec<String> groupIdOpt;
        private final OptionSpec<Integer> fetchSizeOpt;
        private final OptionSpec<String> commandPropertiesOpt;
        private final OptionSpec<Integer> socketBufferSizeOpt;
        @Deprecated(since = "4.2", forRemoval = true)
        private final OptionSpec<String> consumerConfigOpt;
        private final OptionSpec<String> commandConfigOpt;
        private final OptionSpec<Void> printMetricsOpt;
        @Deprecated(since = "4.2", forRemoval = true)
        private final OptionSpec<Long> reportingIntervalOpt;
        private final OptionSpec<String> dateFormatOpt;
        private final OptionSpec<Integer> numThreadsOpt;

        public ShareConsumerPerfOptions(String[] args) {
            super(args);
            bootstrapServerOpt = parser.accepts("bootstrap-server", "REQUIRED. The server(s) to connect to.")
                .withRequiredArg()
                .describedAs("server to connect to")
                .ofType(String.class);
            topicOpt = parser.accepts("topic", "REQUIRED: The topic to consume from.")
                .withRequiredArg()
                .describedAs("topic")
                .ofType(String.class);
            groupIdOpt = parser.accepts("group", "The group id to consume on.")
                .withRequiredArg()
                .describedAs("gid")
                .defaultsTo("perf-share-consumer")
                .ofType(String.class);
            fetchSizeOpt = parser.accepts("fetch-size", "The amount of data to fetch in a single request.")
                .withRequiredArg()
                .describedAs("size")
                .ofType(Integer.class)
                .defaultsTo(1024 * 1024);
            commandPropertiesOpt = parser.accepts("command-property", "Kafka share consumer related configuration properties like client.id. " +
                    "These configs take precedence over those passed via --command-config or --consumer.config.")
                .withRequiredArg()
                .describedAs("prop1=val1")
                .ofType(String.class);
            socketBufferSizeOpt = parser.accepts("socket-buffer-size", "The size of the tcp RECV size.")
                .withRequiredArg()
                .describedAs("size")
                .ofType(Integer.class)
                .defaultsTo(2 * 1024 * 1024);
            consumerConfigOpt = parser.accepts("consumer.config", "(DEPRECATED) Share consumer config properties file. " +
                    "This option will be removed in a future version. Use --command-config instead.")
                .withRequiredArg()
                .describedAs("config file")
                .ofType(String.class);
            commandConfigOpt = parser.accepts("command-config", "Config properties file.")
                .withRequiredArg()
                .describedAs("config file")
                .ofType(String.class);
            printMetricsOpt = parser.accepts("print-metrics", "Print out the metrics.");
            reportingIntervalOpt = parser.accepts("reporting-interval", "Interval in milliseconds at which to print progress info.")
                .withRequiredArg()
                .withValuesConvertedBy(regex("^\\d+$"))
                .describedAs("interval_ms")
                .ofType(Long.class)
                .defaultsTo(5_000L);
            dateFormatOpt = parser.accepts("date-format", "The date format to use for formatting the time field. " +
                    "See java.text.SimpleDateFormat for options.")
                .withRequiredArg()
                .describedAs("date format")
                .ofType(String.class)
                .defaultsTo("yyyy-MM-dd HH:mm:ss:SSS");
            numThreadsOpt = parser.accepts("threads", "The number of share consumers to use for sharing the load.")
                .withRequiredArg()
                .describedAs("count")
                .ofType(Integer.class)
                .defaultsTo(1);
            try {
                options = parser.parse(args);
            } catch (OptionException e) {
                CommandLineUtils.printUsageAndExit(parser, e.getMessage());
                return;
            }
            if (options != null) {
                CommandLineUtils.maybePrintHelpOrVersion(this, "This tool is used to verify the share consumer performance.");
                CommandLineUtils.checkRequiredArgs(parser, options, topicOpt, bootstrapServerOpt);

                CommandLineUtils.checkInvalidArgs(parser, options, consumerConfigOpt, commandConfigOpt);

                if (options.has(consumerConfigOpt)) {
                    System.out.println("Warning: --consumer.config is deprecated. Use --command-config instead.");
                }
            }
        }

        public boolean printMetrics() {
            return options.has(printMetricsOpt);
        }

        public String brokerHostsAndPorts() {
            return options.valueOf(bootstrapServerOpt);
        }

        private Properties readProps(List<String> commandProperties, String commandConfigFile) throws IOException {
            Properties props = commandConfigFile != null
                ? Utils.loadProps(commandConfigFile)
                : new Properties();
            props.putAll(parseKeyValueArgs(commandProperties));
            return props;
        }

        public Properties props() throws IOException {
            List<String> commandProperties = options.valuesOf(commandPropertiesOpt);
            String commandConfigFile;
            if (options.has(consumerConfigOpt)) {
                commandConfigFile = options.valueOf(consumerConfigOpt);
            } else {
                commandConfigFile = options.valueOf(commandConfigOpt);
            }
            Properties props = readProps(commandProperties, commandConfigFile);
            props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, brokerHostsAndPorts());
            props.put(ConsumerConfig.GROUP_ID_CONFIG, options.valueOf(groupIdOpt));
            props.put(ConsumerConfig.RECEIVE_BUFFER_CONFIG, options.valueOf(socketBufferSizeOpt).toString());
            props.put(ConsumerConfig.FETCH_MAX_BYTES_CONFIG, options.valueOf(fetchSizeOpt).toString());
            props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class);
            props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class);
            props.put(ConsumerConfig.CHECK_CRCS_CONFIG, "false");
            if (props.getProperty(ConsumerConfig.CLIENT_ID_CONFIG) == null)
                props.put(ConsumerConfig.CLIENT_ID_CONFIG, "share-consumer-correctness-client");
            return props;
        }

        public Set<String> topic() {
            return Set.of(options.valueOf(topicOpt));
        }

        public int threads() {
            return options.valueOf(numThreadsOpt);
        }

        public long reportingIntervalMs() {
            long value = options.valueOf(reportingIntervalOpt);
            if (value <= 0)
                throw new IllegalArgumentException("Reporting interval must be greater than 0.");
            return value;
        }

        public SimpleDateFormat dateFormat() {
            return new SimpleDateFormat(options.valueOf(dateFormatOpt));
        }
    }

    // Visible for testing.
    record ConsumptionRecordKey(String topic, int partition, long offset) {
        @Override
        public String toString() {
            return "ConsumptionRecordKey(topic = " + topic
                + ", partition = " + partition
                + ", offset = " + offset + ")";
        }
    }

    // Visible for testing.
    record ConsumptionRecordValue(int shareConsumerId, long consumedTimeMs,
                                  int deliveryCount) {
        @Override
        public String toString() {
            return "ConsumerRecord(shareConsumerId = " + shareConsumerId
                + ", consumedTimeMs = " + consumedTimeMs
                + ", deliveryCount = " + deliveryCount + ")";
        }
    }
}
