package com.antares.db.backend.dm.logger;

import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import com.antares.db.backend.utils.Panic;
import com.antares.db.common.Error;

public class LoggerImpl implements Logger {

    private static final int SEED = 13331;

    private static final int OF_SIZE = 0;
    private static final int OF_CHECKSUM = OF_SIZE + 4;
    private static final int OF_DATA = OF_CHECKSUM + 4;

    public static final String LOG_SUFFIX = ".log";

    private RandomAccessFile file;
    private FileChannel fc;
    private Lock lock;

    private long position;  //当前日志指针位置
    private long fileSize;  //日志文件大小
    private int xChecksum; //当前日志校验和

    LoggerImpl(RandomAccessFile raf, FileChannel fc) {
        this.file = raf;
        this.fc = fc;
        this.lock = new ReentrantLock();
    }

    LoggerImpl(RandomAccessFile raf, FileChannel fc, int xChecksum) {
        this.file = raf;
        this.fc = fc;
        this.lock = new ReentrantLock();
        this.xChecksum = xChecksum;
    }

    void init() {
        long size = 0;
        try {
            size = file.length();
        } catch (Exception e) {
            Panic.panic(e);
        }

        if(size < 4) {
            Panic.panic(Error.BadLogFileException);   
        }

        // 读取日志文件头部4个byte的校验和
        ByteBuffer raw = ByteBuffer.allocate(4);
        try {
            fc.position(0);
            fc.read(raw);
        } catch (Exception e) {
            Panic.panic(e);
        }

        // int xChecksum = Parser.parseInt(raw);

    }

    @Override
    public void log(byte[] data) {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'log'");
    }

    @Override
    public void truncate(long x) throws Exception {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'truncate'");
    }

    @Override
    public byte[] next() {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'next'");
    }

    @Override
    public void rewind() {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'rewind'");
    }

    @Override
    public void close() {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'close'");
    }
    
}
