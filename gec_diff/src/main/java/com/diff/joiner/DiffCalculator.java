package com.diff.joiner;

import com.diff.model.DiffRecord;
import com.diff.model.DiffRecord.FieldValue;
import com.diff.model.DiffResult;
import com.diff.model.DiffResult.Op;
import com.diff.model.ValueType;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import java.util.*;

/**
 * 核心 Diff 计算逻辑：
 * 1. 基础类型比较（STRICT / LOOSE 模式）
 * 2. JSON 递归 Diff
 */
public class DiffCalculator {

    public enum Mode { STRICT, LOOSE }

    private final Mode   mode;
    private final double absEpsilon;
    private final double relEpsilon;
    private final Set<String> ignorePaths;

    private static final ObjectMapper MAPPER = new ObjectMapper();

    public DiffCalculator(Mode mode, double absEpsilon, double relEpsilon, Set<String> ignorePaths) {
        this.mode        = mode;
        this.absEpsilon  = absEpsilon;
        this.relEpsilon  = relEpsilon;
        this.ignorePaths = ignorePaths == null ? Collections.emptySet() : ignorePaths;
    }

    /**
     * 对一条 Join 结果（old + new）中的所有字段做 Diff，返回差异列表。
     */
    public List<DiffResult> compute(DiffRecord old, DiffRecord newRec) {
        List<DiffResult> results = new ArrayList<>();
        String prefix = "";

        // 合并所有字段名
        Set<String> allKeys = new LinkedHashSet<>();
        allKeys.addAll(old.fields.keySet());
        allKeys.addAll(newRec.fields.keySet());

        for (String fieldName : allKeys) {
            FieldValue oldVal = old.fields.get(fieldName);
            FieldValue newVal = newRec.fields.get(fieldName);
            String path = "/" + fieldName;

            if (ignorePaths.contains(path)) continue;

            if (oldVal == null) {
                // ADDED
                results.add(buildResult(old, newRec, path, null, newVal, Op.ADDED, 0.0));
            } else if (newVal == null) {
                // REMOVED
                results.add(buildResult(old, newRec, path, oldVal, null, Op.REMOVED, 0.0));
            } else if (oldVal.type == ValueType.JSON && newVal.type == ValueType.JSON) {
                // JSON 递归 Diff
                results.addAll(diffJson(old, newRec, path, oldVal.str, newVal.str));
            } else {
                // 基础类型 Diff
                DiffResult r = diffScalar(old, newRec, path, oldVal, newVal);
                if (r != null) results.add(r);
            }
        }
        return results;
    }

    // ---- JSON 递归 Diff ----
    private List<DiffResult> diffJson(DiffRecord old, DiffRecord newRec,
                                       String basePath, String oldJson, String newJson) {
        List<DiffResult> results = new ArrayList<>();
        try {
            JsonNode oldNode = MAPPER.readTree(oldJson);
            JsonNode newNode = MAPPER.readTree(newJson);
            diffJsonNodes(old, newRec, basePath, oldNode, newNode, results);
        } catch (Exception e) {
            // 解析失败：降级为字符串比较
            if (!oldJson.equals(newJson)) {
                results.add(buildResult(old, newRec, basePath,
                    strField(oldJson), strField(newJson), Op.VALUE_CHANGE, 0.0));
            }
        }
        return results;
    }

    private void diffJsonNodes(DiffRecord old, DiffRecord newRec,
                                String path, JsonNode oldNode, JsonNode newNode,
                                List<DiffResult> results) {
        if (ignorePaths.contains(path)) return;

        if (oldNode.isObject() && newNode.isObject()) {
            Set<String> allFields = new LinkedHashSet<>();
            oldNode.fieldNames().forEachRemaining(allFields::add);
            newNode.fieldNames().forEachRemaining(allFields::add);

            for (String f : allFields) {
                String childPath = path + "/" + f;
                JsonNode ov = oldNode.get(f);
                JsonNode nv = newNode.get(f);
                if (ov == null) {
                    results.add(buildResultJson(old, newRec, childPath, null, nv, Op.ADDED));
                } else if (nv == null) {
                    results.add(buildResultJson(old, newRec, childPath, ov, null, Op.REMOVED));
                } else {
                    diffJsonNodes(old, newRec, childPath, ov, nv, results);
                }
            }
        } else if (oldNode.isArray() && newNode.isArray()) {
            // 有序模式（默认）
            int maxLen = Math.max(oldNode.size(), newNode.size());
            for (int i = 0; i < maxLen; i++) {
                String childPath = path + "/" + i;
                if (i >= oldNode.size()) {
                    results.add(buildResultJson(old, newRec, childPath, null, newNode.get(i), Op.ADDED));
                } else if (i >= newNode.size()) {
                    results.add(buildResultJson(old, newRec, childPath, oldNode.get(i), null, Op.REMOVED));
                } else {
                    diffJsonNodes(old, newRec, childPath, oldNode.get(i), newNode.get(i), results);
                }
            }
        } else {
            // 叶子节点比较
            if (oldNode.getNodeType() != newNode.getNodeType()) {
                results.add(buildResultJson(old, newRec, path, oldNode, newNode, Op.TYPE_CHANGE));
            } else if (!nodesEqual(oldNode, newNode)) {
                results.add(buildResultJson(old, newRec, path, oldNode, newNode, Op.VALUE_CHANGE));
            }
            // else EQUAL，不产出
        }
    }

    private boolean nodesEqual(JsonNode a, JsonNode b) {
        if (a.isNumber() && b.isNumber()) {
            double da = a.doubleValue();
            double db = b.doubleValue();
            return floatEqual(da, db);
        }
        return a.equals(b);
    }

    // ---- 基础类型 Diff ----
    private DiffResult diffScalar(DiffRecord old, DiffRecord newRec,
                                   String path, FieldValue oldVal, FieldValue newVal) {
        // 类型不同
        if (oldVal.type != newVal.type) {
            if (mode == Mode.STRICT) {
                return buildResult(old, newRec, path, oldVal, newVal, Op.TYPE_CHANGE, 0.0);
            }
            // LOOSE 模式：数值类型之间尝试等价比较
            if (isNumeric(oldVal.type) && isNumeric(newVal.type)) {
                double ov = toDouble(oldVal);
                double nv = toDouble(newVal);
                if (!floatEqual(ov, nv)) {
                    return buildResult(old, newRec, path, oldVal, newVal, Op.VALUE_CHANGE,
                        Math.abs(ov - nv));
                }
                return null; // EQUAL
            }
            return buildResult(old, newRec, path, oldVal, newVal, Op.TYPE_CHANGE, 0.0);
        }

        // 同类型比较
        if (isNumeric(oldVal.type)) {
            double ov = toDouble(oldVal);
            double nv = toDouble(newVal);
            if (!floatEqual(ov, nv)) {
                return buildResult(old, newRec, path, oldVal, newVal, Op.VALUE_CHANGE,
                    Math.abs(ov - nv));
            }
            return null; // EQUAL
        }

        if (oldVal.type == ValueType.BOOL) {
            if (!Objects.equals(oldVal.b, newVal.b)) {
                return buildResult(old, newRec, path, oldVal, newVal, Op.VALUE_CHANGE, 0.0);
            }
            return null;
        }

        if (oldVal.type == ValueType.STRING || oldVal.type == ValueType.BYTES) {
            if (!Objects.equals(oldVal.str, newVal.str)) {
                return buildResult(old, newRec, path, oldVal, newVal, Op.VALUE_CHANGE, 0.0);
            }
            return null;
        }

        return null; // NULL_TYPE 等：视为 EQUAL
    }

    // ---- helpers ----
    private boolean floatEqual(double a, double b) {
        double diff = Math.abs(a - b);
        if (diff <= absEpsilon) return true;
        double maxAbs = Math.max(Math.abs(a), Math.abs(b));
        return maxAbs > 0 && diff / maxAbs <= relEpsilon;
    }

    private boolean isNumeric(ValueType t) {
        return t == ValueType.INT32 || t == ValueType.INT64
            || t == ValueType.UINT32 || t == ValueType.UINT64
            || t == ValueType.FLOAT32 || t == ValueType.FLOAT64;
    }

    private double toDouble(FieldValue v) {
        switch (v.type) {
            case INT32:  return v.i32 != null ? v.i32 : 0;
            case INT64:  case UINT32: case UINT64: return v.i64 != null ? v.i64 : 0;
            case FLOAT32: return v.f32 != null ? v.f32 : 0;
            case FLOAT64: return v.f64 != null ? v.f64 : 0;
            default: return 0;
        }
    }

    private DiffResult buildResult(DiffRecord old, DiffRecord newRec, String path,
                                    FieldValue oldVal, FieldValue newVal,
                                    Op op, double absDiff) {
        DiffResult r = new DiffResult();
        r.requestId   = old.requestId;
        r.service     = old.service;
        r.region      = old.region;
        r.key         = old.key;
        r.fieldPath   = path;
        r.op          = op;
        r.oldType     = oldVal != null ? oldVal.type.name().toLowerCase() : "(missing)";
        r.newType     = newVal != null ? newVal.type.name().toLowerCase() : "(missing)";
        r.oldValue    = oldVal != null ? oldVal.toString() : "(missing)";
        r.newValue    = newVal != null ? newVal.toString() : "(missing)";
        r.absDiff     = absDiff;
        r.tags        = old.tags;
        r.timestampMs = old.timestampMs;
        return r;
    }

    private DiffResult buildResultJson(DiffRecord old, DiffRecord newRec, String path,
                                        JsonNode oldNode, JsonNode newNode, Op op) {
        DiffResult r = new DiffResult();
        r.requestId   = old.requestId;
        r.service     = old.service;
        r.region      = old.region;
        r.key         = old.key;
        r.fieldPath   = path;
        r.op          = op;
        r.oldType     = oldNode != null ? oldNode.getNodeType().name().toLowerCase() : "(missing)";
        r.newType     = newNode != null ? newNode.getNodeType().name().toLowerCase() : "(missing)";
        r.oldValue    = oldNode != null ? oldNode.toString() : "(missing)";
        r.newValue    = newNode != null ? newNode.toString() : "(missing)";
        r.absDiff     = (oldNode != null && newNode != null
                         && oldNode.isNumber() && newNode.isNumber())
                        ? Math.abs(oldNode.doubleValue() - newNode.doubleValue()) : 0.0;
        r.tags        = old.tags;
        r.timestampMs = old.timestampMs;
        return r;
    }

    private static FieldValue strField(String s) {
        FieldValue f = new FieldValue();
        f.type = ValueType.STRING;
        f.str  = s;
        return f;
    }
}
