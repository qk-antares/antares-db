package com.antares.db.backend.im;

import java.io.File;
import java.util.List;

import org.junit.Test;

import com.antares.db.backend.dm.DataManager;
import com.antares.db.backend.dm.pageCache.PageCache;
import com.antares.db.backend.tm.MockTransactionManager;
import com.antares.db.backend.tm.TransactionManager;


public class BPlusTreeTest {
    @Test
    public void testTreeSingle() throws Exception {
        TransactionManager tm = new MockTransactionManager();
        DataManager dm = DataManager.create(System.getProperty("user.dir") + "/TestTreeSingle", PageCache.PAGE_SIZE*10, tm);

        long root = BPlusTree.create(dm);
        BPlusTree tree = BPlusTree.load(root, dm);

        int lim = 10000;
        for(int i = lim-1; i >= 0; i --) {
            tree.insert(i, i);
        }

        for(int i = 0; i < lim; i ++) {
            List<Long> uids = tree.search(i);
            assert uids.size() == 1;
            assert uids.get(0) == i;
        }

        assert new File(System.getProperty("user.dir") + "/TestTreeSingle.db").delete();
        assert new File(System.getProperty("user.dir") + "/TestTreeSingle.log").delete();
    }
}
