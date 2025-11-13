package com.antares.db.backend.dm;

import com.antares.db.backend.dm.dateItem.DataItem;
import com.antares.db.backend.dm.logger.Logger;
import com.antares.db.backend.dm.page.PageOne;
import com.antares.db.backend.dm.pageCache.PageCache;
import com.antares.db.backend.tm.TransactionManager;

public interface DataManager {
    DataItem read(long uid) throws Exception;
    long insert(long xid, byte[] data) throws Exception;
    void close();

    public static DataManagerImpl create(String path, long mem, TransactionManager tm) {
        PageCache pc = PageCache.create(path, mem);
        Logger lg = Logger.create(path);

        DataManagerImpl dm = new DataManagerImpl(pc, lg, tm);
        dm.initPageOne();
        return dm;
    }

    public static DataManagerImpl open(String path, long mem, TransactionManager tm) {
        PageCache pc = PageCache.open(path, mem);
        Logger lg = Logger.open(path);
        DataManagerImpl dm = new DataManagerImpl(pc, lg, tm);

        if(!dm.loadCheckPageOne()) {
            Recover.recover(tm, lg, pc);
        }

        dm.fillPageIndex();
        PageOne.setVcOpen(dm.pageOne);
        dm.pc.flushPage(dm.pageOne);
        return dm;
    }
}
