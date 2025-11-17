package com.antares.db.backend.dm;

import com.antares.db.backend.common.AbstractCache;
import com.antares.db.backend.dm.dateItem.DataItem;
import com.antares.db.backend.dm.dateItem.DataItemImpl;
import com.antares.db.backend.dm.logger.Logger;
import com.antares.db.backend.dm.page.Page;
import com.antares.db.backend.dm.page.PageOne;
import com.antares.db.backend.dm.page.PageX;
import com.antares.db.backend.dm.pageCache.PageCache;
import com.antares.db.backend.dm.pageIndex.PageIndex;
import com.antares.db.backend.dm.pageIndex.PageInfo;
import com.antares.db.backend.tm.TransactionManager;
import com.antares.db.backend.utils.Panic;
import com.antares.db.backend.utils.Types;
import com.antares.db.common.Error;

public class DataManagerImpl extends AbstractCache<DataItem> implements DataManager {

    TransactionManager tm;
    PageCache pc;
    Logger logger;
    PageIndex pIndex;
    Page pageOne;

    public DataManagerImpl(PageCache pc, Logger logger, TransactionManager tm) {
        super(0);
        this.pc = pc;
        this.logger = logger;
        this.tm = tm;
        this.pIndex = new PageIndex();
    }

    /**
     * 在创建文件时初始化PageOne(100~107字节随机值)
     */
    void initPageOne() {
        int pgno = pc.newPage(PageOne.initRaw());
        assert pgno == 1;
        try {
            pageOne = pc.getPage(pgno);
        } catch (Exception e) {
            Panic.panic(e);
        }
        pc.flushPage(pageOne);
    }

    /**
     * 在打开已有文件时读入PageOne，并验证正确性
     */
    boolean loadCheckPageOne() {
        try {
            pageOne = pc.getPage(1);
        } catch (Exception e) {
            Panic.panic(e);
        }
        return PageOne.checkVc(pageOne);
    }

    /**
     * 初始化pageIndex，将所有已有页面的信息加载到pageIndex中
     */
    void fillPageIndex() {
        int pageNumber = pc.getPageNumber();
        for(int i = 2; i <= pageNumber; i++) {
            Page pg = null;
            try {
                pg = pc.getPage(i);
            } catch (Exception e) {
                Panic.panic(e);
            }
            pIndex.add(pg.getPageNumber(), PageX.getFreeSpace(pg));
            // getPage后需要release，否则page会被一直缓存，撑爆缓存
            pg.release();
        }
    }

    // region DataManager
    /**
     * 根据uid获取缓存中的DataItem
     * uid: [pgno(4)][空位(2)][offset(2)]
     */
    @Override
    public DataItem read(long uid) throws Exception {
        DataItemImpl di = (DataItemImpl) super.get(uid);
        if(!di.isValid()) {
            di.release();
            return null;
        }
        return di;
    }

    public void releaseDataItem(DataItem di) {
        super.release(di.getUid());
    }

    // 为xid生成update日志
    public void logDataItem(long xid, DataItem di) {
        byte[] log = Recover.updateLog(xid, di);
        logger.log(log);
    }

    @Override
    public long insert(long xid, byte[] data) throws Exception {
        byte[] raw = DataItem.wrapDataItemRaw(data);
        if(raw.length > PageX.MAX_FREE_SPACE) {
            throw Error.DataTooLargeException;
        }

        PageInfo pi = null;
        for(int i = 0; i < 5; i++) {
            pi = pIndex.select(raw.length);
            if(pi != null) {
                break;
            } else {
                // 若没有合适的Page，则新建一个Page
                int pgno = pc.newPage(PageX.initRaw());
                pIndex.add(pgno, PageX.MAX_FREE_SPACE);
            }
        }
        if(pi == null) {
            throw Error.DatabaseBusyException;
        }

        Page pg = null;
        try {
            pg = pc.getPage(pi.pgno);
            byte[] log = Recover.insertLog(xid, pg, raw);
            logger.log(log);

            short offset = PageX.insert(pg, raw);

            pg.release();
            return Types.addressToUid(pi.pgno, offset);
        } finally {
            // 将取出的pg重新插入pIndex
            if(pg != null) {
                pIndex.add(pi.pgno, PageX.getFreeSpace(pg));
            } else {
                pIndex.add(pi.pgno, pi.freeSpace);
            }
        }
    }

    @Override
    public void close() {
        super.close();
        logger.close();

        PageOne.setVcClose(pageOne);
        pageOne.release();
        pc.close();
    }
    // endregion


    // region AbstractCache

    @Override
    protected DataItem getForCache(long uid) throws Exception {
        short offset = (short) (uid & ((1L << 16) - 1));
        uid >>>= 32;
        int pgno = (int) (uid & ((1L << 32) - 1));
        Page pg = pc.getPage(pgno);
        return DataItem.parseDataItem(pg, offset, this);
    }

    @Override
    protected void releaseForCache(DataItem di) {
        di.page().release();
    }
    // endregion
}
