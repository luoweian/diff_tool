package com.diff.joiner;

import com.diff.model.DiffRecord;
import com.diff.model.DiffResult;
import org.apache.flink.api.common.functions.CoGroupFunction;
import org.apache.flink.util.Collector;

import java.util.*;

/**
 * Flink CoGroup 函数：将 OLD 和 NEW 两侧的 DiffRecord 做 Join 并计算 Diff。
 *
 * KeyBy: (request_id + ":" + key)
 * Window: TumblingEventTimeWindows(30s)
 */
public class DiffJoinFunction implements CoGroupFunction<DiffRecord, DiffRecord, DiffResult> {

    private final DiffCalculator calculator;

    public DiffJoinFunction(DiffCalculator calculator) {
        this.calculator = calculator;
    }

    @Override
    public void coGroup(Iterable<DiffRecord> oldRecords,
                        Iterable<DiffRecord> newRecords,
                        Collector<DiffResult> out) {

        List<DiffRecord> olds = toList(oldRecords);
        List<DiffRecord> news = toList(newRecords);

        if (olds.isEmpty() && news.isEmpty()) return;

        // join miss 处理
        if (olds.isEmpty()) {
            // 只有 NEW：产出 ADDED（以 null 作为 OLD 侧）
            emitJoinMiss(news.get(0), DiffRecord.Side.OLD, out);
            return;
        }
        if (news.isEmpty()) {
            // 只有 OLD：产出 REMOVED（以 null 作为 NEW 侧）
            emitJoinMiss(olds.get(0), DiffRecord.Side.NEW, out);
            return;
        }

        // 取各侧最新一条（窗口内理论上只有一条，取 timestampMs 最大）
        DiffRecord oldRec = olds.stream()
            .max(Comparator.comparingLong(r -> r.timestampMs))
            .orElseThrow();
        DiffRecord newRec = news.stream()
            .max(Comparator.comparingLong(r -> r.timestampMs))
            .orElseThrow();

        List<DiffResult> results = calculator.compute(oldRec, newRec);
        for (DiffResult r : results) {
            out.collect(r);
        }
    }

    /**
     * Join miss：对侧缺失时产出一条 join_miss 标记记录。
     */
    private void emitJoinMiss(DiffRecord present, DiffRecord.Side missingSide,
                               Collector<DiffResult> out) {
        DiffResult r = new DiffResult();
        r.requestId   = present.requestId;
        r.service     = present.service;
        r.region      = present.region;
        r.key         = present.key;
        r.fieldPath   = "/__join_miss__";
        r.op          = missingSide == DiffRecord.Side.OLD
                        ? DiffResult.Op.ADDED    // 只有 NEW
                        : DiffResult.Op.REMOVED; // 只有 OLD
        r.oldValue    = missingSide == DiffRecord.Side.OLD ? "(missing)" : "present";
        r.newValue    = missingSide == DiffRecord.Side.NEW ? "(missing)" : "present";
        r.tags        = present.tags;
        r.timestampMs = present.timestampMs;
        out.collect(r);
    }

    private static <T> List<T> toList(Iterable<T> it) {
        List<T> list = new ArrayList<>();
        it.forEach(list::add);
        return list;
    }
}
