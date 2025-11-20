package com.antares.db.backend.im;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import com.antares.db.backend.common.SubArray;
import com.antares.db.backend.dm.dateItem.DataItem;
import com.antares.db.backend.tm.TransactionManagerImpl;
import com.antares.db.backend.utils.Parser;

/**
 * Node结构如下：
 * [LeafFlag(1)][KeyNumber(2)][SibingUid(8)]
 * [Son0][Key0][Son1][Key1]...[SonN][KeyN]
 * 
 * LeafFlag：是否为叶子节点
 * KeyNumber：节点中存储的键的数量
 * SiblingUid：兄弟节点的UID
 * 最后一个KeyN始终为Long.MAX_VALUE
 * 
 * Son是UID指针，而Key是键值
 * 
 * 每个Node存储在DataItem中
 */
public class Node {
    static final int IS_LEAF_OFFSET = 0; // 是否叶子节点
    static final int NO_KEYS_OFFSET = IS_LEAF_OFFSET + 1; // 节点中存储的键的数量
    static final int SIBLING_OFFSET = NO_KEYS_OFFSET + 2; // 兄弟节点的UID
    static final int NODE_HEADER_SIZE = SIBLING_OFFSET + 8; // 节点Header总长

    static final int BALANCE_NUMBER = 32; // 平衡因子，当节点存储2*BALANCE_NUMBER个键时触发分裂
    static final int NODE_SIZE = NODE_HEADER_SIZE + (2 * 8) * (BALANCE_NUMBER * 2 + 2); // 节点总大小，Header+键和指针

    BPlusTree tree;
    DataItem dataItem;
    SubArray raw;
    long uid;

    /**
     * 使用DataManager加载Node
     */
    static Node loadNode(BPlusTree bTree, long uid) throws Exception {
        DataItem di = bTree.dm.read(uid);
        assert di != null;
        Node n = new Node();
        n.tree = bTree;
        n.dataItem = di;
        n.raw = di.data();
        n.uid = uid;
        return n;
    }

    static byte[] newNilRootRaw() {
        SubArray raw = new SubArray(new byte[NODE_SIZE], 0, NODE_SIZE);

        setRawIsLeaf(raw, true);
        setRawNoKeys(raw, 0);
        setRawSibling(raw, 0);

        return raw.raw;
    }

    /**
     * 生成一个根节点
     * 
     * @param left  左子节点uid
     * @param right 右子节点uid
     * @param key   左子节点的初始键值(右为Long.MAX_VALUE)
     * @return
     */
    static byte[] newRootRaw(long left, long right, long key) {
        SubArray raw = new SubArray(new byte[NODE_SIZE], 0, NODE_SIZE);

        setRawIsLeaf(raw, false);
        setRawNoKeys(raw, 2);
        setRawSibling(raw, 0);
        setRawKthSon(raw, left, 0);
        setRawKthKey(raw, key, 0);
        setRawKthSon(raw, right, 1);
        setRawKthKey(raw, Long.MAX_VALUE, 1);

        return raw.raw;
    }

    // region search
    class SearchNextRes {
        long uid;
        long siblingUid;
    }

    /**
     * 寻找对应key所在Node的Next UID（不是找到，只是往子/兄弟节点跳跃）
     */
    public SearchNextRes searchNext(long key) {
        dataItem.rLock();
        try {
            SearchNextRes res = new SearchNextRes();
            int noKeys = getRawNoKeys(raw);
            for (int i = 0; i < noKeys; i++) {
                long ik = getRawKthKey(raw, i);
                // 找到对应子节点
                if (key < ik) {
                    res.uid = getRawKthSon(raw, i);
                    res.siblingUid = 0;
                    return res;
                }
            }
            // 未找到，返回兄弟节点
            res.uid = 0;
            res.siblingUid = getRawSibling(raw);
            return res;
        } finally {
            dataItem.rUnLock();
        }
    }

    class LeafSearchRangeRes {
        List<Long> uids;
        long siblingUid;
    }

    /**
     * 在叶子节点中查找[leftKey, rightKey]范围内的所有UID
     * 
     * @param leftKey
     * @param rightKey
     * @return
     */
    public LeafSearchRangeRes leafSearchRange(long leftKey, long rightKey) {
        dataItem.rLock();
        try {
            int noKeys = getRawNoKeys(raw);
            int kth = 0;
            // 顺序查找
            while (kth < noKeys) {
                long ik = getRawKthKey(raw, kth);
                if (ik >= leftKey) {
                    break;
                }
                kth++;
            }
            List<Long> uids = new ArrayList<>();
            while (kth < noKeys) {
                long ik = getRawKthKey(raw, kth);
                if (ik <= rightKey) {
                    uids.add(getRawKthSon(raw, kth));
                    kth++;
                } else {
                    break;
                }
            }
            // 如果查找到最后一个键，还需要返回兄弟节点UID
            long siblingUid = 0;
            if (kth == noKeys) {
                siblingUid = getRawSibling(raw);
            }
            LeafSearchRangeRes res = new LeafSearchRangeRes();
            res.uids = uids;
            res.siblingUid = siblingUid;
            return res;
        } finally {
            dataItem.rUnLock();
        }
    }
    // endregion

    // region insert
    class InsertAndSplitRes {
        long siblingUid, newSon, newKey;
    }

    public InsertAndSplitRes insertAndSplit(long uid, long key) throws Exception {
        boolean success = false;
        Exception err = null;
        InsertAndSplitRes res = new InsertAndSplitRes();

        dataItem.before();
        try {
            success = insert(uid, key);
            if (!success) {
                res.siblingUid = getRawSibling(raw);
                return res;
            }
            if (needSplit()) {
                try {
                    SplitRes r = split();
                    res.newSon = r.newSon;
                    res.newKey = r.newKey;
                    return res;
                } catch (Exception e) {
                    err = e;
                    throw e;
                }
            } else {
                return res;
            }
        } finally {
            if (err == null && success) {
                dataItem.after(TransactionManagerImpl.SUPER_XID);
            } else {
                dataItem.unBefore();
            }
        }
    }

/**
 * 在当前节点中插入uid-key
 * 
 * @param uid
 * @param key
 * @return
 */
private boolean insert(long uid, long key) {
    // 当前节点保存的键的数量
    int noKeys = getRawNoKeys(raw);
    int kth = 0;
    // Kth是第一个>=key的位置(也即插入位置)
    while (kth < noKeys) {
        long ik = getRawKthKey(raw, kth);
        if (ik < key) {
            kth++;
        } else {
            break;
        }
    }

    // 插入位置在最后，并且有兄弟节点，转移到兄弟节点插入
    if (kth == noKeys && getRawSibling(raw) != 0) {
        return false;
    }

    // kth对应的ik >= key，插入到当前位置
    if (getRawIsLeaf(raw)) {
        // 向右移动
        shiftRawKth(raw, kth);
        setRawKthKey(raw, key, kth);
        setRawKthSon(raw, uid, kth);
        setRawNoKeys(raw, noKeys + 1);
    } else {
        long kk = getRawKthKey(raw, kth);
        setRawKthKey(raw, key, kth);
        shiftRawKth(raw, kth + 1);
        setRawKthKey(raw, kk, kth + 1);
        setRawKthSon(raw, uid, kth + 1);
        setRawNoKeys(raw, noKeys + 1);
    }
    return true;
}

/**
 * 将第kth及之后的键值对向右移动一个槽位(16字节)
 */
static void shiftRawKth(SubArray raw, int kth) {
    int begin = raw.start + NODE_HEADER_SIZE + (kth + 1) * (8 * 2);
    int end = raw.start + NODE_SIZE - 1;
    for(int i = end; i >= begin; i--) {
        raw.raw[i] = raw.raw[i-(8*2)];
    }
}

    private boolean needSplit() {
        return BALANCE_NUMBER * 2 == getRawNoKeys(raw);
    }

    class SplitRes {
        long newSon, newKey;
    }

    /**
     * 分裂当前节点为两个节点，并返回新节点
     * [Node(BALANCE_NUMBER*2)]->[Sibling]
     * ↓
     * [Node(BALANCE_NUMBER)]->[newNode(BALANCE_NUMBER)]->[Sibling(BALANCE_NUMBER)]
     */
    private SplitRes split() throws Exception {
        // 创建一个空节点
        SubArray nodeRaw = new SubArray(new byte[NODE_SIZE], 0, NODE_SIZE);
        setRawIsLeaf(nodeRaw, getRawIsLeaf(raw));
        // 调整节点内容及关系
        setRawNoKeys(nodeRaw, BALANCE_NUMBER);
        setRawSibling(nodeRaw, getRawSibling(raw));
        copyRawFromKth(raw, nodeRaw, BALANCE_NUMBER);
        // 将新节点写入Page
        long son = tree.dm.insert(TransactionManagerImpl.SUPER_XID, nodeRaw.raw);
        setRawNoKeys(raw, BALANCE_NUMBER);
        setRawSibling(raw, son);

        SplitRes res = new SplitRes();
        res.newSon = son;
        res.newKey = getRawKthKey(nodeRaw, 0);
        return res;
    }

    /**
     * 将from节点中第kth及之后的键值对复制到to节点中
     */
    static void copyRawFromKth(SubArray from, SubArray to, int kth) {
        int offset = from.start + NODE_HEADER_SIZE + kth * (8 * 2);
        System.arraycopy(from.raw, offset, to.raw, to.start + NODE_HEADER_SIZE, from.end - offset);
    }

    // endregin

    // region setter
    static void setRawIsLeaf(SubArray raw, boolean isLeaf) {
        if (isLeaf) {
            raw.raw[raw.start + IS_LEAF_OFFSET] = (byte) 1;
        } else {
            raw.raw[raw.start + IS_LEAF_OFFSET] = (byte) 0;
        }
    }

    static void setRawNoKeys(SubArray raw, int noKeys) {
        System.arraycopy(Parser.short2Byte((short) noKeys), 0, raw.raw, raw.start + NO_KEYS_OFFSET, 2);
    }

    static void setRawSibling(SubArray raw, long sibling) {
        System.arraycopy(Parser.long2Byte(sibling), 0, raw.raw, raw.start + SIBLING_OFFSET, 8);
    }

    static void setRawKthSon(SubArray raw, long uid, int kth) {
        int offset = raw.start + NODE_HEADER_SIZE + kth * (8 * 2);
        System.arraycopy(Parser.long2Byte(uid), 0, raw.raw, offset, 8);
    }

    static void setRawKthKey(SubArray raw, long key, int kth) {
        int offset = raw.start + NODE_HEADER_SIZE + kth * (8 * 2) + 8;
        System.arraycopy(Parser.long2Byte(key), 0, raw.raw, offset, 8);
    }

    // endregion

    // region getter
    /**
     * 获取节点中保存的键的数量
     */
    static int getRawNoKeys(SubArray raw) {
        return (int) Parser
                .parseShort(Arrays.copyOfRange(raw.raw, raw.start + NO_KEYS_OFFSET, raw.start + NO_KEYS_OFFSET + 2));
    }

    /**
     * 获取节点中保存的第k个键
     */
    static long getRawKthKey(SubArray raw, int kth) {
        int offset = raw.start + NODE_HEADER_SIZE + kth * (8 * 2) + 8;
        return Parser.parseLong(Arrays.copyOfRange(raw.raw, offset, offset + 8));
    }

    /**
     * 获取节点中保存的第k个子节点UID
     */
    static long getRawKthSon(SubArray raw, int kth) {
        int offset = raw.start + NODE_HEADER_SIZE + kth * (8 * 2);
        return Parser.parseLong(Arrays.copyOfRange(raw.raw, offset, offset + 8));
    }

    /**
     * 获取节点的兄弟节点UID
     */
    static long getRawSibling(SubArray raw) {
        return Parser
                .parseLong(Arrays.copyOfRange(raw.raw, raw.start + SIBLING_OFFSET, raw.start + SIBLING_OFFSET + 8));
    }

    /**
     * 获取节点是否为叶子节点
     */
    static boolean getRawIsLeaf(SubArray raw) {
        return raw.raw[raw.start + IS_LEAF_OFFSET] == (byte) 1;
    }

    public boolean isLeaf() {
        dataItem.rLock();
        try {
            return getRawIsLeaf(raw);
        } finally {
            dataItem.rUnLock();
        }
    }
    // endregion

    public void release() {
        dataItem.release();
    }
}
