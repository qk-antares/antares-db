package com.antares.db.backend.vm;

import java.util.HashMap;
import java.util.Map;

import com.antares.db.backend.tm.TransactionManagerImpl;

/**
 * 事务的抽象
 */
public class Transaction {
    public long xid;
    public int level;
    public Map<Long, Boolean> snapshot; // 事务快照，记录了在该事务开始时刻活跃的事务，TODO 为什么不用Set<Long>
    public Exception err;
    public boolean autoAborted;

    /**
     * 创建一个新的事务对象
     * @param xid
     * @param level 事务隔离级别
     * @param active 当前活跃事务
     * @return
     */
    public static Transaction newTransaction(long xid, int level, Map<Long, Transaction> active) {
        Transaction t = new Transaction();
        t.xid = xid;
        t.level = level;
        if(level != 0) {
            t.snapshot = new HashMap<>();
            for(Long x : active.keySet()) {
                t.snapshot.put(x, true);
            }
        }
        return t;
    }

    public boolean isInSnapshot(long xid) {
        if(xid == TransactionManagerImpl.SUPER_XID) {
            return false;
        }
        return snapshot.containsKey(xid);
    }
}
