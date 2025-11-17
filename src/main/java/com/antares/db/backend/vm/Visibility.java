package com.antares.db.backend.vm;

import com.antares.db.backend.tm.TransactionManager;

public class Visibility {
    /**
     * 检查【版本跳跃】
     * 如果事务t需要修改X，而X已经被t不可见的事务修改了，则要求t回滚
     * 
     * 读提交允许版本跳跃，则可重复读则不允许
     * 下面的repeatableRead并不能确保不发生版本跳跃
     * 
     * @param tm
     * @param t
     * @param e
     * @return
     */
    public static boolean isVersionSkip(TransactionManager tm, Transaction t, Entry e) {
        long xmax = e.getXmax();
        if (t.level == 0) {
            return false;
        } else {
            return tm.isCommitted(xmax) && (xmax > t.xid || t.isInSnapshot(xmax));
        }
    }

    public static boolean isVisible(TransactionManager tm, Transaction t, Entry e) {
        if (t.level == 0) {
            return readCommitted(tm, t, e);
        } else {
            return repeatableRead(tm, t, e);
        }
    }

    /**
     * 读已提交隔离级别下，记录e对事务t是否可见
     * 
     * 读已提交：事务在读取数据时，只能读取已经提交事务产生的数据
     * 
     * 可见有以下几种情况：
     * 1. 由xid创建且未删除
     * 2. 由已提交的事务创建且未删除
     * 3. 由一个未提交的事务删除（还未生效）
     * 
     * 读已提交会产生"不可重复读"现象，即在同一个事务中，两次读取同一数据可能会得到不同的结果
     * 这是因为两次读取之间，另一个事务可能提交了对该数据的修改
     * 
     * @param tm
     * @param t
     * @param e
     * @return
     */
    private static boolean readCommitted(TransactionManager tm, Transaction t, Entry e) {
        long xid = t.xid;
        long xmin = e.getXmin();
        long xmax = e.getXmax();
        if (xmin == xid && xmax == 0) {
            return true;
        }

        if (tm.isCommitted(xmin)) {
            if (xmax == 0) {
                return true;
            }
            if (xmax != xid) {
                if (!tm.isCommitted(xmax)) {
                    return true;
                }
            }
        }
        return false;
    }

    /**
     * 可重复读隔离级别下，记录e对事务t是否可见
     * 
     * 可重复读：事务只能读取它开始时，就已经结束的哪些事务产生的数据版本
     * 即忽略：
     * 1. 在本事务后开始的事务的数据
     * 2. 在本事务开始时还是active状态的事务的数据
     * 
     * 可见有以下几种情况：
     * 1. 由xid创建且未删除
     * 2. 由一个已提交的事务创建，且这个事务<xid，且在xid开始时该事务已提交(不在active中)
     *   且未被删除，或被删除了，但这个事务还未提交 或 这个事务>xid 或 这个事务在xid开始时还未提交(在active中)
     * @param tm
     * @param t
     * @param e
     * @return
     */
    private static boolean repeatableRead(TransactionManager tm, Transaction t, Entry e) {
        long xid = t.xid;
        long xmin = e.getXmin();
        long xmax = e.getXmax();
        if (xmin == xid && xmax == 0) {
            return true;
        }

        if (tm.isCommitted(xmin) && xmin < xid && !t.isInSnapshot(xmin)) {
            if (xmax == 0) {
                return true;
            }
            if (xmax != xid) {
                if (!tm.isCommitted(xmax) || xmax > xid || t.isInSnapshot(xmax)) {
                    return true;
                }
            }
        }
        return false;
    }
}
