package com.antares.db.backend.common;

import java.util.HashMap;
import java.util.Set;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import com.antares.db.common.Error;

public abstract class AbstractCache<T> {
    private HashMap<Long, T> cache; // 实际缓存的数据
    private HashMap<Long, Integer> references; // 引用计数
    private HashMap<Long, Boolean> getting; // 正在被获取的数据

    private int maxResource; // 最大缓存资源数
    private int count = 0; // 当前缓存资源数
    private Lock lock;

    public AbstractCache(int maxResource) {
        this.maxResource = maxResource;
        this.cache = new HashMap<>();
        this.references = new HashMap<>();
        this.getting = new HashMap<>();
        this.lock = new ReentrantLock();
    }

    protected T get(long key) throws Exception {
        while (true) {
            lock.lock();
            // 请求的资源(其数据源)正在被其他线程获取
            if (getting.containsKey(key)) {
                lock.unlock();
                try {
                    Thread.sleep(1);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                    continue;
                }
                continue;
            }

            if (cache.containsKey(key)) {
                // 缓存命中
                T res = cache.get(key);
                references.merge(key, 1, Integer::sum);
                lock.unlock();
                return res;
            }

            // 缓存未命中
            // 缓存已满
            if (maxResource > 0 && count == maxResource) {
                lock.unlock();
                throw Error.CacheFullException;
            }

            // 尝试获取该资源
            count++;
            getting.put(key, true);
            lock.unlock();
            break;
        }

        T obj = null;
        try {
            obj = getForCache(key);
        } catch (Exception e) {
            lock.lock();
            getting.remove(key);
            count--;
            lock.unlock();
            throw e;
        }

        lock.lock();
        cache.put(key, obj);
        references.put(key, 1);
        getting.remove(key);
        lock.unlock();

        return obj;
    }

    /**
     * 释放一个资源
     */
    protected void release(long key) {
        lock.lock();
        try {
            int ref = references.get(key) - 1;
            if (ref == 0) {
                T obj = cache.get(key);
                releaseForCache(obj);
                references.remove(key);
                cache.remove(key);
                count--;
            } else {
                references.put(key, ref);
            }
        } finally {
            lock.unlock();
        }
    }

    /**
     * 关闭缓存，写回所有资源
     */
    protected void close() {
        lock.lock();
        try {
            Set<Long> keys = cache.keySet();
            for (Long key : keys) {
                T obj = cache.get(key);
                releaseForCache(obj);
                references.remove(key);
                cache.remove(key);
            }
        } finally {
            lock.unlock();
        }
    }

    /**
     * 当资源不在缓存时的获取行为
     * 从数据源中去获取资源
     */
    protected abstract T getForCache(long key) throws Exception;

    /**
     * 当资源被驱逐时的写回行为
     */
    protected abstract void releaseForCache(T obj);
}
