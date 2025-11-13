package com.antares.db.backend.dm;

import java.io.File;
import java.security.SecureRandom;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.junit.Test;

import com.antares.db.backend.common.SubArray;
import com.antares.db.backend.dm.dateItem.DataItem;
import com.antares.db.backend.dm.pageCache.PageCache;
import com.antares.db.backend.tm.MockTransactionManager;
import com.antares.db.backend.tm.TransactionManager;
import com.antares.db.backend.utils.Panic;
import com.antares.db.backend.utils.RandomUtil;

public class DataManagerTest {

    static List<Long> uids0, uids1;
    static Lock uidsLock;

    static Random random = new SecureRandom();

    private void initUids() {
        uids0 = new ArrayList<>();
        uids1 = new ArrayList<>();
        uidsLock = new ReentrantLock();
    }

    private void worker(DataManager dm0, DataManager dm1, int tasksNum, int insertRation, CountDownLatch cdl) {
        int dataLen = 60;
        try {
            for (int i = 0; i < tasksNum; i++) {
                int op = Math.abs(random.nextInt()) % 100;
                if (op < insertRation) {
                    byte[] data = RandomUtil.randomBytes(dataLen);
                    long u0, u1 = 0;
                    try {
                        u0 = dm0.insert(0, data);
                    } catch (Exception e) {
                        continue;
                    }
                    try {
                        u1 = dm1.insert(0, data);
                    } catch (Exception e) {
                        Panic.panic(e);
                    }
                    uidsLock.lock();
                    uids0.add(u0);
                    uids1.add(u1);
                    uidsLock.unlock();
                } else {
                    long u0, u1;
                    uidsLock.lock();
                    try {
                        if (uids0.size() == 0) {
                            continue;
                        }
                        int tmp = Math.abs(random.nextInt()) % uids0.size();
                        u0 = uids0.get(tmp);
                        u1 = uids1.get(tmp);
                    } finally {
                        uidsLock.unlock();
                    }

                    DataItem data0 = null, data1 = null;
                    try {
                        data0 = dm0.read(u0);
                    } catch (Exception e) {
                        Panic.panic(e);
                        continue;
                    }
                    if (data0 == null)
                        continue;
                    try {
                        data1 = dm1.read(u1);
                    } catch (Exception e) {
                    }

                    data0.rLock();
                    data1.rLock();
                    SubArray s0 = data0.data();
                    SubArray s1 = data1.data();
                    assert Arrays.equals(Arrays.copyOfRange(s0.raw, s0.start, s0.end),
                            Arrays.copyOfRange(s1.raw, s1.start, s1.end));
                    data0.rUnLock();
                    data1.rUnLock();

                    byte[] newData = RandomUtil.randomBytes(dataLen);
                    data0.before();
                    data1.before();
                    System.arraycopy(newData, 0, s0.raw, s0.start, dataLen);
                    System.arraycopy(newData, 0, s1.raw, s1.start, dataLen);
                    data0.after(0);
                    data1.after(0);
                    data0.release();
                    data1.release();
                }
            }
        } finally {
            cdl.countDown();
        }
    }

    @Test
    public void testDMSingle() throws Exception {
        TransactionManager tm0 = new MockTransactionManager();
        DataManager dm0 = DataManager.create(System.getProperty("user.dir") + "/TESTDMSingle", PageCache.PAGE_SIZE * 10,
                tm0);
        DataManager mdm = MockDataManager.newMockDataManager();

        int tasksNum = 10000;
        CountDownLatch cdl = new CountDownLatch(1);
        initUids();
        Runnable r = () -> worker(dm0, mdm, tasksNum, 50, cdl);
        new Thread(r).run();
        cdl.await();
        dm0.close();
        mdm.close();

        new File(System.getProperty("user.dir") + "/TESTDMSingle.db").delete();
        new File(System.getProperty("user.dir") + "/TESTDMSingle.log").delete();
    }

    @Test
    public void testDMMulti() throws InterruptedException {
        TransactionManager tm0 = new MockTransactionManager();
        DataManager dm0 = DataManager.create(System.getProperty("user.dir") + "/TestDMMulti", PageCache.PAGE_SIZE * 100,
                tm0);
        DataManager mdm = MockDataManager.newMockDataManager();

        int tasksNum = 500;
        CountDownLatch cdl = new CountDownLatch(10);
        initUids();
        for (int i = 0; i < 10; i++) {
            Runnable r = () -> worker(dm0, mdm, tasksNum, 50, cdl);
            new Thread(r).start();
        }
        cdl.await();
        dm0.close();
        mdm.close();

        new File(System.getProperty("user.dir") + "/TestDMMulti.db").delete();
        new File(System.getProperty("user.dir") + "/TestDMMulti.log").delete();
    }

    @Test
    public void testRecoverySimple() throws InterruptedException {
        TransactionManager tm0 = TransactionManager.create(System.getProperty("user.dir") + "/TestRecoverySimple");
        DataManager dm0 = DataManager.create(System.getProperty("user.dir") + "/TestRecoverySimple",
                PageCache.PAGE_SIZE * 30, tm0);
        DataManager mdm = MockDataManager.newMockDataManager();
        dm0.close();

        initUids();
        int workerNums = 10;
        for (int i = 0; i < 8; i++) {
            dm0 = DataManager.open(System.getProperty("user.dir") + "/TestRecoverySimple", PageCache.PAGE_SIZE * 100,
                    tm0);
            CountDownLatch cdl = new CountDownLatch(workerNums);
            for (int k = 0; k < workerNums; k++) {
                final DataManager dm = dm0;
                Runnable r = () -> worker(dm, mdm, 100, 50, cdl);
                new Thread(r).start();
            }
            cdl.await();
        }
        dm0.close();
        mdm.close();

        new File(System.getProperty("user.dir") + "/TestRecoverySimple.db").delete();
        new File(System.getProperty("user.dir") + "/TestRecoverySimple.log").delete();
        new File(System.getProperty("user.dir") + "/TestRecoverySimple.xid").delete();

    }
}
