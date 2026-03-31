package com.diff.joiner;

import com.diff.metrics.MetricsReporter;
import com.diff.model.DiffRecord;
import com.diff.model.DiffResult;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.time.Duration;
import java.util.Arrays;
import java.util.HashSet;

/**
 * Flink Diff Joiner 主入口。
 *
 * 参数（通过 --key value 传入）：
 *   --kafka.bootstrap    Kafka broker 地址
 *   --service.name       服务名（用于 topic 命名）
 *   --window.seconds     Join 窗口大小（默认 30）
 *   --watermark.seconds  Watermark 延迟容忍（默认 5）
 *   --diff.mode          STRICT | LOOSE（默认 STRICT）
 *   --float.abs.epsilon  浮点绝对误差（默认 1e-6）
 *   --float.rel.epsilon  浮点相对误差（默认 1e-4）
 *   --ignore.paths       逗号分隔的忽略路径，如 /timestamp,/trace_id
 *   --clickhouse.url     ClickHouse JDBC URL（可选）
 */
public class DiffFlinkJob {

    public static void main(String[] args) throws Exception {
        ParameterTool params = ParameterTool.fromArgs(args);

        String bootstrapServers = params.get("kafka.bootstrap", "localhost:9092");
        String serviceName      = params.get("service.name", "rank_service");
        int    windowSeconds    = params.getInt("window.seconds", 30);
        int    watermarkSeconds = params.getInt("watermark.seconds", 5);
        String diffModeStr      = params.get("diff.mode", "STRICT");
        double absEpsilon       = params.getDouble("float.abs.epsilon", 1e-6);
        double relEpsilon       = params.getDouble("float.rel.epsilon", 1e-4);
        String ignorePathsStr   = params.get("ignore.paths", "/timestamp,/trace_id,/request_id");

        HashSet<String> ignorePaths = new HashSet<>(Arrays.asList(ignorePathsStr.split(",")));
        DiffCalculator.Mode mode = DiffCalculator.Mode.valueOf(diffModeStr);

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.getConfig().setGlobalJobParameters(params);

        // ---- Kafka Sources ----
        String oldTopic = serviceName + ".diff.old";
        String newTopic = serviceName + ".diff.new";

        KafkaSource<String> oldSource = KafkaSource.<String>builder()
            .setBootstrapServers(bootstrapServers)
            .setTopics(oldTopic)
            .setGroupId("diff-joiner-old")
            .setStartingOffsets(OffsetsInitializer.latest())
            .setValueOnlyDeserializer(new SimpleStringSchema())
            .build();

        KafkaSource<String> newSource = KafkaSource.<String>builder()
            .setBootstrapServers(bootstrapServers)
            .setTopics(newTopic)
            .setGroupId("diff-joiner-new")
            .setStartingOffsets(OffsetsInitializer.latest())
            .setValueOnlyDeserializer(new SimpleStringSchema())
            .build();

        WatermarkStrategy<String> watermarkStrategy = WatermarkStrategy
            .<String>forBoundedOutOfOrderness(Duration.ofSeconds(watermarkSeconds))
            .withTimestampAssigner((event, ts) -> System.currentTimeMillis());

        DataStream<DiffRecord> oldStream = env
            .fromSource(oldSource, watermarkStrategy, "old-source")
            .map(new DiffRecordDeserializer(DiffRecord.Side.OLD));

        DataStream<DiffRecord> newStream = env
            .fromSource(newSource, watermarkStrategy, "new-source")
            .map(new DiffRecordDeserializer(DiffRecord.Side.NEW));

        // ---- Stream Join ----
        DiffCalculator calculator = new DiffCalculator(mode, absEpsilon, relEpsilon, ignorePaths);
        DiffJoinFunction joinFn   = new DiffJoinFunction(calculator);

        DataStream<DiffResult> diffResults = oldStream
            .coGroup(newStream)
            .where(r -> r.requestId + ":" + r.key)
            .equalTo(r -> r.requestId + ":" + r.key)
            .window(TumblingEventTimeWindows.of(Time.seconds(windowSeconds)))
            .apply(joinFn);

        // ---- Metrics + 下游写入 ----
        DataStream<DiffResult> reportedResults = diffResults
            .flatMap(new MetricsReporter())
            .name("metrics-reporter");

        // 打印（开发调试）；生产替换为 ClickHouse / Hive Sink
        reportedResults.print().name("diff-output");

        env.execute("DiffTool-" + serviceName);
    }
}
