package com.antares.db.backend.dm.pageIndex;

/**
 * 每个页面的空闲信息
 */
public class PageInfo {
    public int pgno;
    public int freeSpace;

    public PageInfo(int pgno, int freeSpace) {
        this.pgno = pgno;
        this.freeSpace = freeSpace;
    }
}
