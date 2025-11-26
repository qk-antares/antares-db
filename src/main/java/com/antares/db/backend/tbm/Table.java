package com.antares.db.backend.tbm;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.antares.db.backend.parser.statement.Create;
import com.antares.db.backend.parser.statement.Delete;
import com.antares.db.backend.parser.statement.Insert;
import com.antares.db.backend.parser.statement.Select;
import com.antares.db.backend.parser.statement.Update;
import com.antares.db.backend.parser.statement.Where;
import com.antares.db.backend.tbm.Field.ParseValueRes;
import com.antares.db.backend.tm.TransactionManagerImpl;
import com.antares.db.backend.utils.Panic;
import com.antares.db.backend.utils.ParseStringRes;
import com.antares.db.backend.utils.Parser;
import com.antares.db.common.Error;
import com.google.common.primitives.Bytes;

/**
 * 维护表结构
 * [TableName][NextTable]
 * [Field1Uid][Field2Uid]...[FieldNUid]
 * 
 * TableName:
 * [len(4)][name(len)]
 */
public class Table {
    TableManager tbm;
    long uid;
    String name; // 表名
    long nextUid; // 下一个表的uid
    List<Field> fields; // 字段列表

    public Table(TableManager tbm, long uid) {
        this.tbm = tbm;
        this.uid = uid;
    }

    public Table(TableManager tbm, String name, long nextUid) {
        this.tbm = tbm;
        this.name = name;
        this.nextUid = nextUid;
    }

    public static Table loadTable(TableManager tbm, long uid) {
        byte[] raw = null;
        try {
            raw = ((TableManagerImpl) tbm).vm.read(TransactionManagerImpl.SUPER_XID, uid);
        } catch (Exception e) {
            Panic.panic(e);
        }
        assert raw != null;
        Table tb = new Table(tbm, uid);
        return tb.parseSelf(raw);
    }

    public static Table createTable(TableManager tbm, long nextUid, long xid, Create create) throws Exception {
        Table tb = new Table(tbm, create.tableName, nextUid);
        for (int i = 0; i < create.fieldName.length; i++) {
            String fieldName = create.fieldName[i];
            String fieldType = create.fieldType[i];
            boolean indexed = false;
            for (int j = 0; j < create.index.length; j++) {
                if (fieldName.equals(create.index[j])) {
                    indexed = true;
                    break;
                }
            }
            tb.fields.add(Field.createField(tb, xid, fieldName, fieldType, indexed));
        }

        return tb.persistSelf(xid);
    }

    /**
     * 插入一条记录
     */
    public void insert(long xid, Insert insert) throws Exception {
        // 转换为字段-值映射
        Map<String, Object> entry = string2Entry(insert.values);
        byte[] raw = entry2Raw(entry);
        long uid = ((TableManagerImpl) tbm).vm.insert(xid, raw);
        for (Field f : fields) {
            if (f.isIndexed()) {
                f.insert(entry.get(f.fieldName), uid);
            }
        }
    }

    /**
     * 读取记录
     */
    public String read(long xid, Select read) throws Exception {
        List<Long> uids = parseWhere(read.where);
        StringBuilder sb = new StringBuilder();
        for (Long uid : uids) {
            byte[] raw = ((TableManagerImpl) tbm).vm.read(xid, uid);
            if (raw == null) {
                continue;
            }
            Map<String, Object> entry = parseEntry(raw);
            sb.append(printEntry(entry)).append("\n");
        }
        return sb.toString();
    }

    /**
     * 更新记录(只支持单字段更新)
     */
    public int update(long xid, Update update) throws Exception {
        // 满足条件的记录uid
        List<Long> uids = parseWhere(update.where);
        Field fd = null;
        for (Field f : fields) {
            if (f.fieldName.equals(update.fieldName)) {
                fd = f;
                break;
            }
        }
        if (fd == null) {
            throw Error.FieldNotFoundException;
        }

        Object value = fd.string2Value(update.value);
        int count = 0;
        for (Long uid : uids) {
            byte[] raw = ((TableManagerImpl) tbm).vm.read(xid, uid);
            if (raw == null) {
                continue;
            }
            // 直接删除
            ((TableManagerImpl) tbm).vm.delete(xid, uid);

            // 更新值，重新插入
            Map<String, Object> entry = parseEntry(raw);
            entry.put(update.fieldName, value);
            raw = entry2Raw(entry);
            long uuid = ((TableManagerImpl) tbm).vm.insert(xid, raw);
            count++;

            // TODO: 更新索引(为什么是直接插入)
            for (Field f : fields) {
                if (f.isIndexed()) {
                    f.insert(entry.get(f.fieldName), uuid);
                }
            }
        }
        return count;
    }

    /**
     * 删除记录
     */
    public int delete(long xid, Delete delete) throws Exception {
        // 满足条件的记录uid
        List<Long> uids = parseWhere(delete.where);
        int count = 0;
        for (Long uid : uids) {
            if(((TableManagerImpl) tbm).vm.delete(xid, uid)) {
                count++;
            }
        }
        return count;
    }

    // region private
    private Table parseSelf(byte[] raw) {
        int position = 0;
        ParseStringRes res = Parser.parseString(raw);
        name = res.str;
        position += res.next;
        nextUid = Parser.parseLong(Arrays.copyOfRange(raw, position, position + 8));
        position += 8;

        while (position < raw.length) {
            long uid = Parser.parseLong(Arrays.copyOfRange(raw, position, position + 8));
            position += 8;
            fields.add(Field.loadField(this, uid));
        }
        return this;
    }

    private Table persistSelf(long xid) throws Exception {
        byte[] nameRaw = Parser.string2Byte(name);
        byte[] nextRaw = Parser.long2Byte(nextUid);
        byte[] fieldRaw = new byte[0];
        for (Field f : fields) {
            fieldRaw = Bytes.concat(fieldRaw, Parser.long2Byte(f.uid));
        }
        this.uid = ((TableManagerImpl) tbm).vm.insert(xid, Bytes.concat(nameRaw, nextRaw, fieldRaw));
        return this;
    }

    class CalWhereRes {
        long l0, r0, l1, r1;
        boolean single;
    }

    /**
     * 解析where条件，只有构建了索引的字段才能作为where条件，返回符合条件的uid列表
     * 
     * @param where
     * @return
     * @throws Exception
     */
    private List<Long> parseWhere(Where where) throws Exception {
        long l0 = 0, r0 = 0, l1 = 0, r1 = 0;
        boolean single = false;
        Field fd = null;
        if (where == null) {
            for (Field f : fields) {
                if (f.isIndexed()) {
                    fd = f;
                    break;
                }
            }
            l0 = 0;
            r0 = Long.MAX_VALUE;
            single = true;
        } else {
            for (Field f : fields) {
                if (f.fieldName.equals(where.singleExp1.field)) {
                    if (!f.isIndexed()) {
                        throw Error.FieldNotIndexedException;
                    }
                    fd = f;
                    break;
                }
            }
            if (fd == null) {
                throw Error.FieldNotFoundException;
            }
            CalWhereRes res = calWhere(fd, where);
            l0 = res.l0;
            r0 = res.r0;
            l1 = res.l1;
            ;
            r1 = res.r1;
            single = res.single;
        }

        List<Long> uids = fd.search(l0, r0);
        if (!single) {
            List<Long> uids2 = fd.search(l1, r1);
            uids.addAll(uids2);
        }
        return uids;
    }

    private CalWhereRes calWhere(Field fd, Where where) throws Exception {
        CalWhereRes res = new CalWhereRes();
        switch (where.logicOp) {
            case "":
                res.single = true;
                FieldCalRes r = fd.calExp(where.singleExp1);
                res.l0 = r.left;
                res.r0 = r.right;
                break;
            case "or":
                res.single = false;
                r = fd.calExp(where.singleExp1);
                res.l0 = r.left;
                res.r0 = r.right;
                r = fd.calExp(where.singleExp2);
                res.l1 = r.left;
                res.r1 = r.right;
                break;
            case "and":
                res.single = true;
                r = fd.calExp(where.singleExp1);
                res.l0 = r.left;
                res.r0 = r.right;
                r = fd.calExp(where.singleExp2);
                res.l1 = r.left;
                res.r1 = r.right;
                if (res.l1 > res.l0) {
                    res.l0 = res.l1;
                }
                if (res.r1 < res.r0) {
                    res.r0 = res.r1;
                }
                break;
            default:
                throw Error.InvalidLogOpException;
        }
        return res;
    }

    /**
     * 解析记录内容
     */
    private Map<String, Object> parseEntry(byte[] raw) {
        Map<String, Object> entry = new HashMap<>();
        int pos = 0;
        for (Field f : fields) {
            ParseValueRes r = f.parseValue(Arrays.copyOfRange(raw, pos, raw.length));
            entry.put(f.fieldName, r.v);
            pos += r.shift;
        }
        return entry;
    }

    // region utils
    private Map<String, Object> string2Entry(String[] values) throws Exception {
        if (values.length != fields.size()) {
            throw Error.InvalidValuesException;
        }
        Map<String, Object> entry = new HashMap<>();
        for (int i = 0; i < fields.size(); i++) {
            Field f = fields.get(i);
            Object v = f.string2Value(values[i]);
            entry.put(f.fieldName, v);
        }
        return entry;
    }

    private byte[] entry2Raw(Map<String, Object> entry) {
        byte[] raw = new byte[0];
        for (Field f : fields) {
            raw = Bytes.concat(raw, f.value2Raw(entry.get(f.fieldName)));
        }
        return raw;
    }

    private String printEntry(Map<String, Object> entry) {
        StringBuilder sb = new StringBuilder("[");
        for (int i = 0; i < fields.size(); i++) {
            Field f = fields.get(i);
            sb.append(f.printValue(entry.get(f.fieldName)));
            if (i == fields.size() - 1) {
                sb.append("]");
            } else {
                sb.append(", ");
            }
        }

        return sb.toString();
    }
}
