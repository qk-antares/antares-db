package com.antares.db.backend.dm.page;

import java.util.Arrays;

import com.antares.db.backend.dm.pageCache.PageCache;
import com.antares.db.backend.utils.RandomUtil;

/**
 * 特殊管理第一页，用于存储元信息
 * 
 * ValidCheck
 * db启动时给100~107字节填入一个随机值，db关闭时将其拷贝到108~115字节
 * 再次启动时根据100~107和108~115字节判断db是否正常关闭
 */
public class PageOne {
    private static final int OF_VC = 100; // ValidCheck起始偏移
    private static final int LEN_VC = 8;  // ValidCheck长度

    public static byte[] initRaw() {
        byte[] raw = new byte[PageCache.PAGE_SIZE];
        setVcOpen(raw);
        return raw;
    }

    /**
     * 在db启动时，设置100~107字节的随机值
     * @param pg
     */
    public static void setVcOpen(Page pg) {
        pg.setDirty(true);
        setVcOpen(pg.getData());
    }

    private static void setVcOpen(byte[] raw) {
        System.arraycopy(RandomUtil.randomBytes(LEN_VC), 0, raw, OF_VC, LEN_VC);
    }

    public static void setVcClose(Page pg) {
        pg.setDirty(true);
        setVcClose(pg.getData());
    }

    private static void setVcClose(byte[] raw) {
        System.arraycopy(raw, OF_VC, raw, OF_VC + LEN_VC, LEN_VC);
    }

    public static boolean checkVc(Page pg) {
        return checkVc(pg.getData());
    }

    private static boolean checkVc(byte[] raw) {
        return Arrays.equals(Arrays.copyOfRange(raw, OF_VC, OF_VC+LEN_VC), Arrays.copyOfRange(raw, OF_VC+LEN_VC, OF_VC+2*LEN_VC));
    }
}
