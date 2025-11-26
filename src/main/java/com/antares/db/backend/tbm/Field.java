package com.antares.db.backend.tbm;

import java.util.Arrays;
import java.util.List;

import com.antares.db.backend.im.BPlusTree;
import com.antares.db.backend.parser.statement.SingleExpression;
import com.antares.db.backend.tm.TransactionManagerImpl;
import com.antares.db.backend.utils.Panic;
import com.antares.db.backend.utils.ParseStringRes;
import com.antares.db.backend.utils.Parser;
import com.antares.db.common.Error;
import com.google.common.primitives.Bytes;

/**
 * 字段信息
 * [FieldName][TypeName][IndexUid(8)]
 * 如果field无索引，IndexUid=0
 * 
 * FiledName:
 * [len(4)][name(len)]
 * 
 * TypeName:
 * [len(4)][name(len)]
 */
public class Field {
    long uid;
    private Table tb;
    String fieldName;
    String fieldType;
    private long index;
    private BPlusTree bt;

    public Field(long uid, Table tb) {
        this.uid = uid;
        this.tb = tb;
    }

    public Field(Table tb, String fieldName, String fieldType, long index) {
        this.tb = tb;
        this.fieldName = fieldName;
        this.fieldType = fieldType;
        this.index = index;
    }

    public static Field loadField(Table tb, long uid) {
        byte[] raw = null;
        try {
            raw = ((TableManagerImpl) tb.tbm).vm.read(TransactionManagerImpl.SUPER_XID, uid);
        } catch (Exception e) {
            com.antares.db.backend.utils.Panic.panic(e);
        }
        assert raw != null;
        return new Field(uid, tb).parseSelf(raw);
    }

    public static Field createField(Table tb, long xid, String fieldName, String fieldType, boolean indexed)
            throws Exception {
        typeCheck(fieldType);
        Field f = new Field(tb, fieldName, fieldType, 0);
        if (indexed) {
            // index是B+树的bootUid
            long index = BPlusTree.create(((TableManagerImpl) tb.tbm).dm);
            BPlusTree bt = BPlusTree.load(index, ((TableManagerImpl) tb.tbm).dm);
            f.index = index;
            f.bt = bt;
        }
        f.persistSelf(xid);
        return f;
    }

    /**
     * 将键值插入索引
     * 
     * @param key 字段值，string类型要做特殊处理
     * @param uid 记录uid(一整行)
     * @throws Exception
     */
    public void insert(Object key, long uid) throws Exception {
        long uKey = value2Uid(key);
        bt.insert(uKey, uid);
    }

    public FieldCalRes calExp(SingleExpression exp) throws Exception {
        Object v = null;
        FieldCalRes res = new FieldCalRes();
        switch (exp.compareOp) {
            case "<":
                res.left = 0;
                v = string2Value(exp.value);
                res.right = value2Uid(v);
                // TODO: 为什么？要减也是正负一起减
                if (res.right > 0) {
                    res.right--;
                }
                break;
            case "=":
                v = string2Value(exp.value);
                res.left = value2Uid(v);
                res.right = res.left;
                break;
            case ">":
                res.right = Long.MAX_VALUE;
                v = string2Value(exp.value);
                // TODO: 这里又是不管正负一起+1
                res.left = value2Uid(v) + 1;
                break;
        }
        return res;
    }

    /**
     * 范围搜索，返回uid
     * 
     * @param left
     * @param right
     * @return
     * @throws Exception
     */
    public List<Long> search(long left, long right) throws Exception {
        return bt.searchRange(left, right);
    }

    class ParseValueRes {
        Object v;
        int shift;
    }

    public ParseValueRes parseValue(byte[] raw) {
        ParseValueRes res = new ParseValueRes();
        switch (fieldType) {
            case "int32":
                res.v = Parser.parseInt(Arrays.copyOf(raw, 4));
                res.shift = 4;
                break;
            case "int64":
                res.v = Parser.parseLong(Arrays.copyOf(raw, 8));
                res.shift = 8;
                break;
            case "string":
                ParseStringRes r = Parser.parseString(raw);
                res.v = r.str;
                res.shift = r.next;
                break;
        }
        return res;
    }

    // region private
    private Field parseSelf(byte[] raw) {
        int position = 0;
        ParseStringRes res = Parser.parseString(raw);
        fieldName = res.str;
        position += res.next;

        res = Parser.parseString(Arrays.copyOfRange(raw, position, raw.length));
        fieldType = res.str;
        position += res.next;

        index = Parser.parseLong(Arrays.copyOfRange(raw, position, position + 8));
        if (index != 0) {
            try {
                bt = BPlusTree.load(index, ((TableManagerImpl) tb.tbm).dm);
            } catch (Exception e) {
                Panic.panic(e);
            }
        }

        return this;
    }

    /**
     * 持久化字段
     */
    private void persistSelf(long xid) throws Exception {
        byte[] nameRaw = Parser.string2Byte(fieldName);
        byte[] typeRaw = Parser.string2Byte(fieldType);
        byte[] indexRaw = Parser.long2Byte(index);
        this.uid = ((TableManagerImpl) tb.tbm).vm.insert(xid, Bytes.concat(nameRaw, typeRaw, indexRaw));
    }

    // region utils
    private static void typeCheck(String fieldType) throws Exception {
        if (!"int32".equals(fieldType) && !"int64".equals(fieldType) && !"string".equals(fieldType)) {
            throw Error.InvalidFieldException;
        }
    }

    public Object string2Value(String str) {
        switch (fieldType) {
            case "int32":
                return Integer.parseInt(str);
            case "int64":
                return Long.parseLong(str);
            case "string":
                return str;
        }
        return null;
    }

    public byte[] value2Raw(Object v) {
        byte[] raw = null;
        switch (fieldType) {
            case "int32":
                raw = Parser.int2Byte((Integer) v);
                break;
            case "int64":
                raw = Parser.long2Byte((Long) v);
                break;
            case "string":
                raw = Parser.string2Byte((String) v);
                break;
        }
        return raw;
    }

    public long value2Uid(Object key) {
        long uid = 0;
        switch (fieldType) {
            case "string":
                uid = Parser.str2Uid((String) key);
                break;
            case "int32":
                int uint = (int) key;
                return (long) uint;
            case "int64":
                uid = (long) key;
                break;
        }
        return uid;
    }

    public String printValue(Object v) {
        String str = null;
        switch (fieldType) {
            case "int32":
                str = String.valueOf((int) v);
                break;
            case "int64":
                str = String.valueOf((long) v);
                break;
            case "string":
                str = (String) v;
                break;
        }
        return str;
    }

    // region getter
    public boolean isIndexed() {
        return index != 0;
    }
}
