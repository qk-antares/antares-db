package com.antares.db.backend.dm.pageCache;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import com.antares.db.backend.common.AbstractCache;
import com.antares.db.backend.dm.page.Page;
import com.antares.db.backend.dm.page.PageImpl;
import com.antares.db.backend.utils.Panic;
import com.antares.db.common.Error;

public class PageCacheImpl extends AbstractCache<Page> implements PageCache {
    private static final int MEM_MIN_LIMIT = 10;
    public static final String DB_SUFFIX = ".db";

    private RandomAccessFile file;
    private FileChannel fc;
    private Lock fileLock;

    private AtomicInteger pageNumbers;

    PageCacheImpl(RandomAccessFile file, FileChannel fileChannel, int maxResource) {
        super(maxResource);
        if (maxResource < MEM_MIN_LIMIT) {
            Panic.panic(Error.MemTooSmallException);
        }

        long length = 0;
        try {
            length = file.length();
        } catch (Exception e) {
            Panic.panic(e);
        }

        this.file = file;
        this.fc = fileChannel;
        this.pageNumbers = new AtomicInteger((int) length / PAGE_SIZE); // 计算当前已有的页数
        this.fileLock = new ReentrantLock();
    }

    // region PageCache

    @Override
    public int newPage(byte[] initData) {
        int pgno = pageNumbers.incrementAndGet();
        Page page = new PageImpl(pgno, initData, null);
        try {
            flushPage(page);
        } catch (Exception e) {
            Panic.panic(e);
        }
        return pgno;
    }

    @Override
    public Page getPage(int pgno) throws Exception {
        return super.get((long) pgno);
    }

    @Override
    public int getPageNumber() {
        return pageNumbers.get();
    }

    @Override
    public void release(Page page) {
        super.release((long) page.getPageNumber());
    }

    @Override
    public void truncateByPgno(int maxPgno) {
        long size = pageOffset(maxPgno+1);
        try {
            file.setLength(size);
        } catch (IOException e) {
            Panic.panic(e);
        }
        pageNumbers.set(maxPgno);
    }

    @Override
    public void flushPage(Page pg) {
        int pgno = pg.getPageNumber();
        long offset = pageOffset(pgno);

        fileLock.lock();
        try {
            ByteBuffer buf = ByteBuffer.wrap(pg.getData());
            fc.position(offset);
            fc.write(buf);
            fc.force(false);
        } catch (IOException e) {
            Panic.panic(e);
        } finally {
            fileLock.unlock();
        }
    }

    @Override
    public void close() {
        super.close();
        try {
            fc.close();
            file.close();
        } catch (IOException e) {
            Panic.panic(e);
        }
    }

    // endregion

    // region AbstractCache

    /**
     * 根据pageNumber从数据库文件中读取页数据，并包装成Page
     */
    @Override
    protected Page getForCache(long key) throws Exception {
        int pgno = (int) key;
        long offset = PageCacheImpl.pageOffset(pgno);

        ByteBuffer buf = ByteBuffer.allocate(PAGE_SIZE);
        fileLock.lock();
        try {
            fc.position(offset);
            fc.read(buf);
        } catch(IOException e) {
            Panic.panic(e);
        }finally {
            fileLock.unlock();
        }

        return new PageImpl(pgno, buf.array(), this);
    }

    @Override
    protected void releaseForCache(Page pg) {
        if(pg.isDirty()) {
            flushPage(pg);
        }
    }

    // endregion

    // region Utils

    private static long pageOffset(int pgno) {
        return (pgno - 1) * PAGE_SIZE;
    }

    // endregion
}
