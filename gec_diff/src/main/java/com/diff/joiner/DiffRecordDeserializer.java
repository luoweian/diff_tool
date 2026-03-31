package com.diff.joiner;

import com.diff.model.DiffRecord;
import com.diff.model.DiffRecord.FieldValue;
import com.diff.model.ValueType;
import org.apache.flink.api.common.functions.MapFunction;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;

/**
 * 将 MQ 消息（简易二进制格式）反序列化为 DiffRecord。
 * 格式与 C++ AsyncWriter::Serialize() 对应。
 *
 * 生产环境替换为 Protobuf 反序列化。
 */
public class DiffRecordDeserializer implements MapFunction<String, DiffRecord> {

    private final DiffRecord.Side expectedSide;

    public DiffRecordDeserializer(DiffRecord.Side expectedSide) {
        this.expectedSide = expectedSide;
    }

    @Override
    public DiffRecord map(String raw) throws Exception {
        // 实际消息为 bytes，这里 payload 以 base64 或直接 bytes 传入
        // 简化：假设 raw 为 JSON 格式（生产中用 protobuf）
        // TODO: 替换为真实 protobuf 反序列化
        return parseSimpleBinary(raw.getBytes(StandardCharsets.ISO_8859_1));
    }

    private DiffRecord parseSimpleBinary(byte[] data) {
        Reader r = new Reader(data);
        DiffRecord rec = new DiffRecord();

        rec.requestId = r.readString();
        rec.service   = r.readString();
        rec.region    = r.readString();
        rec.side      = r.readByte() == 1 ? DiffRecord.Side.OLD : DiffRecord.Side.NEW;
        rec.key       = r.readString();

        int tagCount = r.readInt();
        rec.tags = new HashMap<>(tagCount);
        for (int i = 0; i < tagCount; i++) {
            String k = r.readString();
            String v = r.readString();
            rec.tags.put(k, v);
        }

        rec.timestampMs = r.readLong();

        int fieldCount = r.readInt();
        rec.fields = new HashMap<>(fieldCount);
        for (int i = 0; i < fieldCount; i++) {
            String name       = r.readString();
            ValueType type    = ValueType.fromCode(r.readByte() & 0xFF);
            byte[] valueBytes = r.readBytes();
            FieldValue fv     = decodeField(type, valueBytes);
            rec.fields.put(name, fv);
        }

        return rec;
    }

    private FieldValue decodeField(ValueType type, byte[] bytes) {
        FieldValue fv = new FieldValue();
        fv.type     = type;
        fv.rawBytes = bytes;

        ByteBuffer buf = ByteBuffer.wrap(bytes).order(ByteOrder.LITTLE_ENDIAN);
        switch (type) {
            case INT32:   fv.i32 = buf.getInt();    break;
            case INT64:   fv.i64 = buf.getLong();   break;
            case UINT32:  fv.i64 = (long) buf.getInt() & 0xFFFFFFFFL; break;
            case UINT64:  fv.i64 = buf.getLong();   break;
            case FLOAT32: fv.f32 = buf.getFloat();  break;
            case FLOAT64: fv.f64 = buf.getDouble(); break;
            case BOOL:    fv.b   = bytes.length > 0 && bytes[0] != 0; break;
            case STRING:
            case BYTES:
            case JSON:
                fv.str = new String(bytes, StandardCharsets.UTF_8);
                break;
            default: break;
        }
        return fv;
    }

    // ---- 简易二进制 Reader ----
    private static class Reader {
        private final ByteBuffer buf;

        Reader(byte[] data) {
            buf = ByteBuffer.wrap(data).order(ByteOrder.LITTLE_ENDIAN);
        }

        String readString() {
            int len = buf.getInt();
            byte[] bytes = new byte[len];
            buf.get(bytes);
            return new String(bytes, StandardCharsets.UTF_8);
        }

        byte[] readBytes() {
            int len = buf.getInt();
            byte[] bytes = new byte[len];
            buf.get(bytes);
            return bytes;
        }

        int readInt()  { return buf.getInt(); }
        long readLong() { return buf.getLong(); }
        byte readByte() { return buf.get(); }
    }
}
