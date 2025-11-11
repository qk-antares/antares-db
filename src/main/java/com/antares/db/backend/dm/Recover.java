package com.antares.db.backend.dm;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.antares.db.backend.dm.dateItem.DataItem;
import com.antares.db.backend.dm.logger.Logger;
import com.antares.db.backend.dm.page.Page;
import com.antares.db.backend.dm.page.PageX;
import com.antares.db.backend.dm.pageCache.PageCache;
import com.antares.db.backend.tm.TransactionManager;
import com.antares.db.backend.utils.Panic;
import com.antares.db.backend.utils.Parser;

/**
 * 每一条日志的格式：
 * UPDATE日志:
 * [LogType] [XID] [UID] [OldRaw] [NewRaw]
 * UID的高32bit存储Pgno，低16bit存储Offset，即
 * UID: [Pgno] [None] [Offset]
 * 
 * INSERT日志:
 * [LogType] [XID] [Pgno] [Offset] [Raw]
 */
public class Recover {
    private static final byte LOG_TYPE_INSERT = 0;
    private static final byte LOG_TYPE_UPDATE = 1;

    private static final int REDO = 0;
    private static final int UNDO = 1;

    private static final int OF_TYPE = 0;
    private static final int OF_XID = OF_TYPE + 1;

    private static final int OF_UPDATE_UID = OF_XID + 8;
    private static final int OF_UPDATE_RAW = OF_UPDATE_UID + 8;

    private static final int OF_INSERT_PGNO = OF_XID + 8;
    private static final int OF_INSERT_OFFSET = OF_INSERT_PGNO + 4;
    private static final int OF_INSERT_RAW = OF_INSERT_OFFSET + 2;

    static class InsertLogInfo {
        long xid;
        int pgno;
        short offset;
        byte[] raw;
    }

    static class UpdateLogInfo {
        long xid;
        int pgno;
        short offset;
        byte[] oldRaw;
        byte[] newRaw;
    }

    public static void recover(TransactionManager tm, Logger lg, PageCache pc) {
        System.out.println("Recovering...");

        lg.rewind();
        int maxPgno = 0;
        while (true) {
            byte[] log = lg.next();
            if (log == null)
                break;

            int pgno;
            if (isInsertLog(log)) {
                InsertLogInfo li = parseInsertLog(log);
                pgno = li.pgno;
            } else {
                UpdateLogInfo li = parseUpdateLog(log);
                pgno = li.pgno;
            }

            if (pgno > maxPgno) {
                maxPgno = pgno;
            }
        }

        if (maxPgno == 0) {
            maxPgno = 1;    // PageOne保留的是元信息
        }
        pc.truncateByPgno(maxPgno);
        System.out.println("Truncate to " + maxPgno + " pages.");

        redoTransactions(tm, lg, pc);
        System.out.println("Redo Transactions finished.");

        undoTransactions(tm, lg, pc);
        System.out.println("Undo Transactions finished.");

        System.out.println("Recover finished.");
    }

    private static boolean isInsertLog(byte[] log) {
        return log[0] == LOG_TYPE_INSERT;
    }

    private static InsertLogInfo parseInsertLog(byte[] log) {
        InsertLogInfo li = new InsertLogInfo();
        li.xid = Parser.parseLong(Arrays.copyOfRange(log, OF_XID, OF_INSERT_PGNO));
        li.pgno = Parser.parseInt(Arrays.copyOfRange(log, OF_INSERT_PGNO, OF_INSERT_OFFSET));
        li.offset = Parser.parseShort(Arrays.copyOfRange(log, OF_INSERT_OFFSET, OF_INSERT_RAW));
        li.raw = Arrays.copyOfRange(log, OF_INSERT_RAW, log.length);
        return li;
    }

    private static UpdateLogInfo parseUpdateLog(byte[] log) {
        UpdateLogInfo li = new UpdateLogInfo();
        li.xid = Parser.parseLong(Arrays.copyOfRange(log, OF_XID, OF_UPDATE_UID));
        long uid = Parser.parseLong(Arrays.copyOfRange(log, OF_UPDATE_UID, OF_UPDATE_RAW));
        li.offset = (short) (uid & ((1L << 16) - 1));
        uid >>>= 32;
        li.pgno = (int) (uid & ((1L << 32) - 1));
        int length = (log.length - OF_UPDATE_RAW) / 2;
        li.oldRaw = Arrays.copyOfRange(log, OF_UPDATE_RAW, OF_UPDATE_RAW + length);
        li.newRaw = Arrays.copyOfRange(log, OF_UPDATE_RAW + length, log.length);
        return li;
    }

    private static void redoTransactions(TransactionManager tm, Logger lg, PageCache pc) {
        lg.rewind();
        while (true) {
            byte[] log = lg.next();
            if (log == null)
                break;

            if (isInsertLog(log)) {
                InsertLogInfo li = parseInsertLog(log);
                if (!tm.isActive(li.xid)) {
                    doInsertLog(pc, log, REDO);
                }
            } else {
                UpdateLogInfo li = parseUpdateLog(log);
                if (!tm.isActive(li.xid)) {
                    doUpdateLog(pc, log, REDO);
                }
            }
        }
    }

    private static void undoTransactions(TransactionManager tm, Logger lg, PageCache pc) {
        Map<Long, List<byte[]>> logCache = new HashMap<>();
        lg.rewind();
        while (true) {
            byte[] log = lg.next();
            if (log == null)
                break;

            if (isInsertLog(log)) {
                InsertLogInfo li = parseInsertLog(log);
                if (tm.isActive(li.xid)) {
                    if (!logCache.containsKey(li.xid)) {
                        logCache.put(li.xid, new ArrayList<>());
                    }
                    logCache.get(li.xid).add(log);
                }
            } else {
                UpdateLogInfo li = parseUpdateLog(log);
                if (tm.isActive(li.xid)) {
                    if (!logCache.containsKey(li.xid)) {
                        logCache.put(li.xid, new ArrayList<>());
                    }
                    logCache.get(li.xid).add(log);
                }
            }
        }

        // 对所有active log进行倒序undo
        for (Map.Entry<Long, List<byte[]>> entry : logCache.entrySet()) {
            List<byte[]> logs = entry.getValue();
            for (int i = logs.size() - 1; i >= 0; i--) {
                byte[] log = logs.get(i);
                if (isInsertLog(log)) {
                    doInsertLog(pc, log, UNDO);
                } else {
                    doUpdateLog(pc, log, UNDO);
                }
            }
        }
    }

    private static void doInsertLog(PageCache pc, byte[] log, int flag) {
        InsertLogInfo li = parseInsertLog(log);
        Page pg = null;
        try {
            pg = pc.getPage(li.pgno);
        } catch (Exception e) {
            Panic.panic(e);
        }

        try {
            if (flag == UNDO) {
                DataItem.setDataItemRawInvalid(li.raw);
            }
            PageX.recoverInsert(pg, li.raw, li.offset);
        } finally {
            pg.release();
        }
    }

    private static void doUpdateLog(PageCache pc, byte[] log, int flag) {
        UpdateLogInfo li = parseUpdateLog(log);      
        int pgno = li.pgno;
        short offset = li.offset;
        byte[] raw;
        if (flag == REDO) {
            raw = li.newRaw;
        } else {
            raw = li.oldRaw;
        }

        Page pg = null;
        try {
            pg = pc.getPage(pgno);
        } catch (Exception e) {
            Panic.panic(e);
        }

        try {
            PageX.recoverUpdate(pg, raw, offset);
        } finally {
            pg.release();
        }
    }

}
