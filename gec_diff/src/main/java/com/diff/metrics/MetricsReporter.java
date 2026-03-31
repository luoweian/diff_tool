package com.diff.metrics;

import com.diff.model.DiffResult;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.Gauge;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.util.Collector;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.LongAdder;

/**
 * 将 DiffResult 转为 Flink Metrics 打点。
 *
 * 按 (service, region, key, field_path, op) 维度聚合：
 *   diff.total         - 总 Join 次数
 *   diff.diff_count    - 差异次数（非 EQUAL）
 *   diff.join_miss     - join miss 次数
 *   diff.type_change   - type_change 次数
 */
public class MetricsReporter extends RichFlatMapFunction<DiffResult, DiffResult> {

    // key = "service|region|key|field_path" → counters
    private final ConcurrentHashMap<String, LongAdder> totalCounters     = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, LongAdder> diffCounters      = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, LongAdder> joinMissCounters  = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, LongAdder> typeChangeCounters = new ConcurrentHashMap<>();

    @Override
    public void open(Configuration parameters) {
        MetricGroup mg = getRuntimeContext().getMetricGroup();

        // 注册全局聚合 gauge（按当前窗口内 counters 求和）
        mg.gauge("diff.total_fields_processed", () ->
            totalCounters.values().stream().mapToLong(LongAdder::sum).sum());
        mg.gauge("diff.total_diff_count", () ->
            diffCounters.values().stream().mapToLong(LongAdder::sum).sum());
        mg.gauge("diff.join_miss_count", () ->
            joinMissCounters.values().stream().mapToLong(LongAdder::sum).sum());
        mg.gauge("diff.type_change_count", () ->
            typeChangeCounters.values().stream().mapToLong(LongAdder::sum).sum());
    }

    @Override
    public void flatMap(DiffResult result, Collector<DiffResult> out) {
        String dimKey = buildDimKey(result);

        totalCounters.computeIfAbsent(dimKey, k -> new LongAdder()).increment();

        switch (result.op) {
            case VALUE_CHANGE:
            case TYPE_CHANGE:
            case ADDED:
            case REMOVED:
                diffCounters.computeIfAbsent(dimKey, k -> new LongAdder()).increment();
                break;
            default:
                break;
        }

        if (result.op == DiffResult.Op.TYPE_CHANGE) {
            typeChangeCounters.computeIfAbsent(dimKey, k -> new LongAdder()).increment();
        }

        if ("/__join_miss__".equals(result.fieldPath)) {
            joinMissCounters.computeIfAbsent(dimKey, k -> new LongAdder()).increment();
        }

        // 透传给下游（写 ClickHouse / Hive）
        out.collect(result);
    }

    private String buildDimKey(DiffResult r) {
        return r.service + "|" + r.region + "|" + r.key + "|" + r.fieldPath;
    }
}
