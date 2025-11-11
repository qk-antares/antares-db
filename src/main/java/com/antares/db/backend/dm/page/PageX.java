package com.antares.db.backend.dm.page;

import java.util.Arrays;

import com.antares.db.backend.dm.pageCache.PageCache;
import com.antares.db.backend.utils.Parser;

/*
 * 普通页
 */
public class PageX {
    // 开头的两个字节存储空闲指针，指向本页中第一个空闲的byte
    private static final short OF_FREE = 0;
    private static final short OF_DATA = 2;
    public static final int MAX_FREE_SPACE = PageCache.PAGE_SIZE - OF_DATA;

    public static byte[] initRaw() {
        byte[] raw = new byte[PageCache.PAGE_SIZE];
        // 初始化时，整个数据区都是空闲的
        setFSO(raw, OF_DATA);
        return raw;
    }

    /*
     * 把ofData写入页数据的前两个字节，表示空闲指针
     */
    private static void setFSO(byte[] raw, short ofData) {
        System.arraycopy(Parser.short2Byte(ofData), 0, raw, OF_FREE, OF_DATA);
    }

    public static short getFSO(Page pg) {
        return getFSO(pg.getData());
    }

    /*
     * 读取页数据前两个字节，获取空闲指针
     */
    private static short getFSO(byte[] raw) {
        return Parser.parseShort(Arrays.copyOfRange(raw, OF_FREE, OF_DATA));
    }

    /*
     * 将raw插入pg中，返回插入位置的偏移量
     * 
     * TODO: raw长度不能超过页中剩余的空闲空间
     */
    public static short insert(Page pg, byte[] raw) {
        pg.setDirty(true);
        short offset = getFSO(pg.getData());
        System.arraycopy(raw, 0, pg.getData(), offset, raw.length);
        setFSO(pg.getData(), (short)(offset + raw.length));
        return offset;
    }

    /*
     * 获取页中剩余的空闲空间大小
     */
    public static int getFreeSpace(Page pg) {
        return PageCache.PAGE_SIZE - getFSO(pg.getData());
    }

    // region 数据恢复相关方法

    /*
     * 将raw插入pg的offset位置，并将pg的空闲指针设置为较大的offset
     * 
     * 也就是说有两种情况：
     * 情况1：offset+raw.length位置在当前空闲指针之后，则将空闲指针后移
     * 情况2：offset+raw.length位置在当前空闲指针之前（也就是说覆盖了前面的数据），则不修改空闲指针
     */
    public static void recoverInsert(Page pg, byte[] raw, short offset) {
        pg.setDirty(true);
        System.arraycopy(raw, 0, pg.getData(), offset, raw.length);

        short rawFSO = getFSO(pg.getData());
        if(rawFSO < offset + raw.length) {
            setFSO(pg.getData(), (short)(offset + raw.length));
        }
    }

    /**
     * 直接将raw插入pg的offset位置，不修改空闲指针
     */
    public static void recoverUpdate(Page pg, byte[] raw, short offset) {
        pg.setDirty(true);
        System.arraycopy(raw, 0, pg.getData(), offset, raw.length);
    }

    // endregion
}
