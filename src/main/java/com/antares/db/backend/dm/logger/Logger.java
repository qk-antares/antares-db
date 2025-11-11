package com.antares.db.backend.dm.logger;

import java.io.File;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

import com.antares.db.backend.utils.Panic;
import com.antares.db.backend.utils.Parser;
import com.antares.db.common.Error;

public interface Logger {
    /**
     * 记录一条日志
     */
    void log(byte[] data);

    /**
     * 截断日志文件到x位置
     */
    void truncate(long x) throws Exception;

    /**
     * 读取下一条日志
     */
    byte[] next();

    /**
     * 将日志指针回退到xChecksum之后
     */
    void rewind();

    void close();

    public static LoggerImpl create(String path) {
        File f = new File(path + LoggerImpl.LOG_SUFFIX);
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
        } catch (Exception e) {
            Panic.panic(e);
        }

        ByteBuffer buf = ByteBuffer.wrap(Parser.int2Byte(0));
        try {
            fc.position(0);
            fc.write(buf);
            fc.force(false);
        } catch (Exception e) {
            Panic.panic(e);
        }

        return new LoggerImpl(raf, fc, 0);
    }

    public static LoggerImpl open(String path) {
        File f = new File(path + LoggerImpl.LOG_SUFFIX);
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

        LoggerImpl lg = new LoggerImpl(raf, fc);
        lg.init();

        return lg;
    }
}
