package com.antares.db.backend.vm;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import com.antares.db.common.Error;

/**
 * 维护依赖等待图，以进行死锁检测
 */
public class LockTable {
    private Map<Long, List<Long>> x2u; // 某个XID已经获得的dataItem的UID列表
    private Map<Long, Long> u2x; // UID被某个XID持有，和x2u是反向映射
    private Map<Long, List<Long>> wait;; // 正在等待UID的XID列表
    private Map<Long, Long> waitU; // XID正在等待的UID，和wait是反向映射
    private Map<Long, Lock> waitLock; // 正在等待资源的XID的锁
    private Lock lock;  // 锁的是整个LockTable

    public LockTable() {
        x2u = new HashMap<>();
        u2x = new HashMap<>();
        wait = new HashMap<>();
        waitLock = new HashMap<>();
        waitU = new HashMap<>();
        lock = new ReentrantLock();
    }

    /**
     * 向依赖等待图中添加一条边xid->uid
     * 
     * 不需要等待则返回null，否则返回锁对象
     * 会造成死锁则抛出异常
     */
    public Lock add(long xid, long uid) throws Exception {
        lock.lock();
        try {
            // 该xid已经持有uid，无需等待
            if(isInList(x2u, xid, uid)) {
                return null;
            }
            // uid没有被持有，直接分配
            if(!u2x.containsKey(uid)) {
                u2x.put(uid, xid);
                putIntoList(x2u, xid, uid);
                return null;
            }

            // uid被持有，检查是否会造成死锁
            waitU.put(xid, uid);
            putIntoList(wait, uid, xid);
            if(hasDeadLock()) {
                waitU.remove(xid);
                removeFromList(wait, uid, xid);
                throw Error.DeadlockException;
            }
            Lock l = new ReentrantLock();
            l.lock();
            waitLock.put(xid, l);
            return l;
        } finally {
            lock.unlock();
        }
    }

    /**
     * 当一个事务commit或abort时，释放它持有的所有资源，并唤醒等待它的事务
     */
    public void remove(long xid) {
        lock.lock();
        try {
            // 该xid持有的uid列表
            List<Long> l = x2u.get(xid);
            if(l != null) {
                while(l.size() > 0) {
                    Long uid = l.remove(0);
                    selectNewXID(uid);
                }
            }
            waitU.remove(xid);
            x2u.remove(xid);
            waitLock.remove(xid);
        } finally {
            lock.unlock();
        }
    }

    /**
     * 从等待队列中选择一个xid来占用uid
     */
    private void selectNewXID(long uid) {
        u2x.remove(uid);
        // 等待该uid的xid列表
        List<Long> l = wait.get(uid);
        if(l == null) return;
        assert l.size() > 0;

        while(l.size() > 0) {
            Long xid = l.remove(0);
            // 该xid已经调用过remove(commit/abort)，跳过，因为remove函数中没有将xid从wait.get(uid)中删除
            if(!waitLock.containsKey(xid)) {
                continue;
            } else {
                u2x.put(uid, xid);
                // TODO 漏写了x2u的逻辑
                putIntoList(x2u, xid, uid);
                Lock lo = waitLock.remove(xid);
                waitU.remove(xid);
                lo.unlock();
                break;
            }
        }

        if(l.size() == 0) {
            wait.remove(uid);
        }
    }

    /**
     * 检查listMap中uid0对应的列表中是否包含uid1
     */
    private boolean isInList(Map<Long, List<Long>> listMap, long uid0, long uid1) {
        List<Long> l = listMap.get(uid0);
        if (l == null) {
            return false;
        }
        Iterator<Long> i = l.iterator();
        while (i.hasNext()) {
            long e = i.next();
            if(e == uid1) {
                return true;
            }
        }
        return false;
    }

    /**
     * 将uid1加入到listMap中uid0对应的列表中
     */
    private void putIntoList(Map<Long, List<Long>> listMap, long uid0, long uid1) {
        if(!listMap.containsKey(uid0)) {
            listMap.put(uid0, new ArrayList<Long>());
        }
        listMap.get(uid0).add(uid1);
    }

    /**
     * 将uid1从listMap中uid0对应的列表中删除
     */
    private void removeFromList(Map<Long, List<Long>> listMap, long uid0, long uid1) {
        List<Long> l = listMap.get(uid0);
        if (l == null) {
            return;
        }
        Iterator<Long> i = l.iterator();
        while (i.hasNext()) {
            long e = i.next();
            if(e == uid1) {
                i.remove();
                break;
            }
        }
        if(l.size() == 0) {
            listMap.remove(uid0);
        }
    }

    private Map<Long, Integer> xidStamp;    // dfs时对xid的标记（该xid是哪次dfs时被访问到的）
    private int stamp;                  // 当前dfs的标记
    /**
     * 检查是否存在死锁
     * 
     * 为每个节点设置一个访问戳，初始化为-1
     * 随后遍历所有节点，以每个非-1的节点作为根进行dfs，并将该连通图中遇到的所有节点都设置为同一个数据
     * 在dfs过程中，如果遇到了相同stamp的节点，则说明存在环，发生死锁
     */
    private boolean hasDeadLock() {
        xidStamp = new HashMap<>();
        stamp = 1;
        for(long xid : x2u.keySet()) {
            Integer s = xidStamp.get(xid);
            // TODO s>0的判断是必须的吗
            if(s != null && s > 0) {
                continue;
            }
            // TODO 放到中间和放到末尾有什么区别？
            stamp++;
            if(dfs(xid)) {
                return true;
            }
        } 
        return false;
    }

    private boolean dfs(long xid) {
        Integer stp = xidStamp.get(xid);
        if(stp != null && stp == stamp) {
            return true;
        }
        if(stp != null && stp < stamp) {
            return false;
        }
        xidStamp.put(xid, stamp);

        Long uid = waitU.get(xid);
        if(uid == null) {
            return false;
        }
        Long x = u2x.get(uid);
        assert x != null;
        return dfs(x);
    }
}
