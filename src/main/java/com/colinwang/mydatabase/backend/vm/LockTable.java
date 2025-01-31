package com.colinwang.mydatabase.backend.vm;

import com.colinwang.mydatabase.common.Error;

import java.util.*;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * 维护了一个依赖等待图，以进行死锁检测
 */
public class LockTable {
    private Map<Long, List<Long>> x2u;  // 某个XID已经获得的资源的UID列表
    private Map<Long, Long> u2x;        // UID被某个XID持有
    private Map<Long, List<Long>> wait; // 正在等待UID的XID列表
    private Map<Long, Lock> waitLock;   // 正在等待资源的XID的锁
    private Map<Long, Long> waitU;      // XID正在等待的UID
    private Lock lock;

    /**
     * 调用add()实现阻塞案例：
     * Lock l = lt.add(xid, uid);
     * if(l != null) {
     *     l.lock();   // 阻塞在这一步
     *     l.unlock();
     * }
     */
    public Lock add(long xid, long uid) throws Exception {
        lock.lock();
        try {
            // 检查事务是否已经持有该资源，如果持有则返回null
            if (isInList(x2u, xid, uid)) {
                return null;
            }
            // 资源未被占用
            if (!u2x.containsKey(uid)) {
                u2x.put(uid, xid);
                putIntoList(x2u, xid, uid);
                return null;
            }
            // 资源已被占用，创建新锁并返回，用于阻塞当前事务
            waitU.put(xid, uid);
            putIntoList(wait, uid, xid);
            if (hasDeadLock()) {
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

    public void remove(long xid) {
        lock.lock();
        try {
            List<Long> list = x2u.get(xid);
            if (list != null) {
                for (Long uid : list) {
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

    // 从等待队列中选择一个xid来占用uid
    private void selectNewXID(long uid) {
        u2x.remove(uid);
        List<Long> list = wait.get(uid);
        if (list == null) return;
        assert !list.isEmpty();

        while (!list.isEmpty()) {
            long xid = list.remove(0);
            if (!waitLock.containsKey(xid)) {
                continue;
            } else {
                u2x.put(uid, xid);
                Lock lk = waitLock.get(xid);
                waitU.remove(xid);
                lk.unlock();
                break;
            }
        }

        if (list.isEmpty()) wait.remove(uid);
    }

    private Set<Long> visited;

    private boolean hasDeadLock() {
        visited = new HashSet<>();
        for (long xid : x2u.keySet()) {
            if (!visited.contains(xid)) {
                if (dfs(xid, new HashSet<>())) {
                    return true;
                }
            }
        }
        return false;
    }

    private boolean dfs(long xid, Set<Long> path) {
        if (path.contains(xid)) {
            return true; // 形成回环，死锁
        }
        if (visited.contains(xid)) {
            return false;
        }
        path.add(xid);
        visited.add(xid);

        Long uid = waitU.get(xid);
        if (uid != null) {
            Long x = u2x.get(uid);
            if (x != null && dfs(x, path)) {
                return true;
            }
        }
        path.remove(xid);
        return false;
    }

    private void putIntoList(Map<Long, List<Long>> listMap, long uid0, long uid1) {
        if (!listMap.containsKey(uid0)) {
            listMap.put(uid0, new ArrayList<Long>());
        }
        listMap.get(uid0).add(0, uid1);
    }

    private boolean isInList(Map<Long, List<Long>> listMap, long uid0, long uid1) {
        List<Long> list = listMap.get(uid0);
        if (list == null) {
            return false;
        }
        for (Long uid : list) {
            if (uid1 == uid) {
                return true;
            }
        }
        return false;
    }

    private void removeFromList(Map<Long, List<Long>> listMap, long uid0, long uid1) {
        List<Long> list = listMap.get(uid0);
        if(list == null) return;
        Iterator<Long> i = list.iterator();
        while (i.hasNext()) {
            Long uid = i.next();
            if (uid == uid1) {
                i.remove();
                break;
            }
        }
        if (list.isEmpty()) {
            listMap.remove(uid0);
        }
    }
}