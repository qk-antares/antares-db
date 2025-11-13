package com.antares.db.backend.dm.pageIndex;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import com.antares.db.backend.dm.pageCache.PageCache;

public class PageIndex {
    // 将一页划分成40个区间
    private static final int INTERVALS_NO = 40;
    private static final int THRESHOLD = PageCache.PAGE_SIZE / INTERVALS_NO;

    private Lock lock;
    // lists[i]存放的是有i个区间空闲的PageInfo
    private List<PageInfo>[] lists;

    @SuppressWarnings("unchecked")
    public PageIndex() {
        lock = new ReentrantLock();
        lists = new List[INTERVALS_NO + 1];
        for (int i = 0; i < INTERVALS_NO + 1; i++) {
            lists[i] = new ArrayList<>();
        }
    }

    /**
     * 添加一个页面的空闲信息
     * @param pgno
     * @param freeSpace
     */
    public void add(int pgno, int freeSpace) {
        lock.lock();
        try {
            int number = freeSpace / THRESHOLD;
            lists[number].add(new PageInfo(pgno, freeSpace));
        } finally {
            lock.unlock();
        }
    }

    /**
     * spaceSize是需要的空间
     * 
     * @param spaceSize
     * @return
     */
    public PageInfo select(int spaceSize) {
        lock.lock();
        try {
            // 计算所需的区间（向上取整）
            int number = spaceSize / THRESHOLD;
            if (number < INTERVALS_NO)
                number++;
            while (number <= INTERVALS_NO) {
                if (lists[number].isEmpty()) {
                    number++;
                    continue;
                }
                // 被选择的页直接从PageIndex中移除，不允许并发写，上层模块使用完这个页面后，需要将其重新插入PageIndex
                return lists[number].remove(0);
            }
            return null;
        } finally {
            lock.unlock();
        }
    }
}
