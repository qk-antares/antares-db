package com.antares.db.backend.dm.dateItem;

import java.util.Arrays;

import com.antares.db.backend.common.SubArray;
import com.antares.db.backend.dm.DataManagerImpl;
import com.antares.db.backend.dm.page.Page;
import com.antares.db.backend.utils.Parser;
import com.antares.db.backend.utils.Types;
import com.google.common.primitives.Bytes;

/**
 * DataItem
 * [valid(1)] [size(2)][data(size)]
 */
public interface DataItem {
    SubArray data();

    void before();

    void unBefore();

    void after(long xid);

    void release();

    void lock();

    void unlock();

    void rLock();

    void rUnLock();

    Page page();

    long getUid();

    byte[] getOldRaw();

    SubArray getRaw();

    /**
     * 包装DataItem的raw数据
     */
    public static byte[] wrapDataItemRaw(byte[] raw) {
        byte[] valid = new byte[1];
        byte[] size = Parser.short2Byte((short) raw.length);
        return Bytes.concat(valid, size, raw);
    }

    /**
     * 从Page的offset处解析出DataItem
     * @param pg
     * @param offset
     * @param dm
     * @return
     */
    public static DataItem parseDataItem(Page pg, short offset, DataManagerImpl dm) {
        byte[] raw = pg.getData();
        // 解析出DataItem的size和length
        short size = Parser.parseShort(Arrays.copyOfRange(raw, offset+DataItemImpl.OF_SIZE, offset+DataItemImpl.OF_DATA));
        short length = (short) (size + DataItemImpl.OF_DATA);
        long uid = Types.addressToUid(pg.getPageNumber(), offset);
        return new DataItemImpl(new SubArray(raw, offset, offset+length), new byte[length], pg, uid, dm);
    }

    public static void setDataItemRawInvalid(byte[] raw) {
        raw[DataItemImpl.OF_VALID] = (byte) 1;
    }
}
