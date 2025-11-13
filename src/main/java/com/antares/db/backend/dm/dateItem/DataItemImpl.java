package com.antares.db.backend.dm.dateItem;

import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import com.antares.db.backend.common.SubArray;
import com.antares.db.backend.dm.DataManagerImpl;
import com.antares.db.backend.dm.page.Page;

/**
 * [ValidFlag(1)][Size(2)][Data(size)]
 */
public class DataItemImpl implements DataItem {
    static final int OF_VALID = 0;
    static final int OF_SIZE = 1;
    static final int OF_DATA = 3;

    private SubArray raw;   // 当前数据
    private byte[] oldRaw;  // 旧数据
    private DataManagerImpl dm;
    private long uid;   // [pgno(4)][空位(2)[offset(2)]]
    private Page pg;    // 所属的Page

    private Lock rLock;
    private Lock wLock;

    public DataItemImpl(SubArray raw, byte[] oldRaw, Page pg, long uid, DataManagerImpl dm) {
        this.raw = raw;
        this.oldRaw = oldRaw;
        this.pg = pg;
        this.uid = uid;
        this.dm = dm;

        ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
        this.rLock = lock.readLock();
        this.wLock = lock.writeLock();
    }

    public boolean isValid() {
        return raw.raw[raw.start + OF_VALID] == 0;
    }

    @Override
    public SubArray data() {
        return new SubArray(raw.raw, raw.start+OF_DATA, raw.end);
    }

    /**
     * 修改DataItem之前的操作：
     * 获取写锁，标记所属Page为脏页，将当前数据内容备份到oldRaw，用于后续回滚
     */
    @Override
    public void before() {
        wLock.lock();
        pg.setDirty(true);
        System.arraycopy(raw.raw, raw.start, oldRaw, 0, oldRaw.length);
    }

    /**
     * 回滚DataItem：
     * 将oldRaw的数据拷贝回raw中，释放写锁
     */
    @Override
    public void unBefore() {
        System.arraycopy(oldRaw, 0, raw.raw, raw.start, oldRaw.length);
        wLock.unlock();
    }

    /**
     * 修改DataItem提交时的操作：
     * 记录日志，释放写锁
     */
    @Override
    public void after(long xid) {
        dm.logDataItem(xid, this);
        wLock.unlock();
    }

    @Override
    public void release() {
        dm.releaseDataItem(this);
    }

    @Override
    public void lock() {
        wLock.lock();
    }

    @Override
    public void unlock() {
        wLock.unlock();
    }

    @Override
    public void rLock() {
        rLock.lock();
    }

    @Override
    public void rUnLock() {
        rLock.unlock();
    }

    @Override
    public Page page() {
        return pg;
    }

    @Override
    public long getUid() {
        return uid;
    }

    @Override
    public byte[] getOldRaw() {
        return oldRaw;
    }

    @Override
    public SubArray getRaw() {
        return raw;
    }
}
