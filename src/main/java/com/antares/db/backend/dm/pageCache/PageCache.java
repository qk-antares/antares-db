package com.antares.db.backend.dm.pageCache;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.RandomAccessFile;
import java.nio.channels.FileChannel;

import com.antares.db.backend.dm.page.Page;
import com.antares.db.backend.utils.Panic;
import com.antares.db.common.Error;

public interface PageCache {
    // 页面大小，8KB
    public static final int PAGE_SIZE = 1 << 13;

    /**
     * 创建一个新页面，并初始化数据，返回新页面的页号。
     * 
     * @param initData 页面初始化数据
     * @return 新页面的页号
     */
    int newPage(byte[] initData);

    /**
     * 根据页号获取页面对象。
     * 
     * @param pgno 页号
     * @return 页面对象
     * @throws Exception 获取失败时抛出异常
     */
    Page getPage(int pgno) throws Exception;

    /**
     * 获取当前缓存或数据库中的页面总数。
     * 
     * @return 页面数量
     */
    int getPageNumber();

    /**
     * 释放页面对象（用于引用计数）。
     * 
     * @param page 要释放的页面
     */
    void release(Page page);

    /**
     * 按页号截断数据库文件，删除大于maxPgno的页面。
     * 
     * @param maxPgno 最大保留的页号
     */
    void truncateByPgno(int maxPgno);

    /**
     * 将页面数据刷新（写回）到磁盘。
     * 
     * @param page 要刷新的页面
     */
    void flushPage(Page page);

    /**
     * 关闭页面缓存，释放相关资源。
     */
    void close();

    /**
     * 创建一个新的页面缓存实例
     */
    public static PageCacheImpl create(String path, long memory) {
        File f = new File(path + PageCacheImpl.DB_SUFFIX);
        try {
            if (!f.createNewFile()) {
                Panic.panic(Error.FileExistsException);
            }
        } catch (Exception e) {
            Panic.panic(e);
        }

        if (!f.canRead() || !f.canWrite()) {
            Panic.panic(Error.FileCannotRWException);
        }

        FileChannel fc = null;
        RandomAccessFile raf = null;
        try {
            raf = new RandomAccessFile(f, "rw");
            fc = raf.getChannel();
        } catch (FileNotFoundException e) {
            Panic.panic(e);
        }
        return new PageCacheImpl(raf, fc, (int) memory / PAGE_SIZE);
    }

    /**
     * 打开一个已有的页面缓存实例
     */
    public static PageCache open(String path, long memory) {
        File f = new File(path + PageCacheImpl.DB_SUFFIX);
        if (!f.exists()) {
            Panic.panic(Error.FileNotExistsException);
        }
        if (!f.canRead() || !f.canWrite()) {
            Panic.panic(Error.FileCannotRWException);
        }

        FileChannel fc = null;
        RandomAccessFile raf = null;
        try {
            raf = new RandomAccessFile(f, "rw");
            fc = raf.getChannel();
        } catch (Exception e) {
            Panic.panic(e);
        }
        return new PageCacheImpl(raf, fc, (int) memory / PAGE_SIZE);
    }
}
