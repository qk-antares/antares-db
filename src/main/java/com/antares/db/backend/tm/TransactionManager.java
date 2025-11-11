package com.antares.db.backend.tm;

import java.io.File;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

import com.antares.db.backend.utils.Panic;
import com.antares.db.common.Error;

/*
 * 是无辜那里其TransactionManager维护一个XID格式的文件，用来记录各个事务的状态
 * 事务状态包括：活跃、已提交、已回滚
 */
public interface TransactionManager {
    long begin();   // 开始事务，返回事务ID
    void commit(long xid); // 提交事务
    void abort(long xid);  // 回滚事务
    boolean isActive(long xid); // 检查事务是否活跃
    boolean isCommitted(long xid); // 检查事务是否已提交
    boolean isAborted(long xid);  // 检查事务是否已回滚
    void close(); // 关闭事务管理器

    public static TransactionManagerImpl create(String path) {
        File f = new File(path + TransactionManagerImpl.XID_SUFFIX);
        try {
            if(!f.createNewFile()) {
                Panic.panic(Error.FileExistsException);
            }
        } catch (Exception e) {
            Panic.panic(e);
        }

        if(!f.canRead() || !f.canWrite()) {
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

        // 写空XID文件头
        ByteBuffer buf = ByteBuffer.allocate(TransactionManagerImpl.LEN_XID_HEADER_LENGTH);
        try {
            fc.position(0);
            fc.write(buf);
        } catch (Exception e) {
            Panic.panic(e);
        }

        return new TransactionManagerImpl(raf, fc);
    }

    public static TransactionManagerImpl open(String path) {
        File f = new File(path + TransactionManagerImpl.XID_SUFFIX);
        if(!f.exists()) {
            Panic.panic(Error.FileNotExistsException);
        }
        if(!f.canRead() || !f.canWrite()) {
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

        return new TransactionManagerImpl(raf, fc);
    }
}
