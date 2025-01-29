package com.colinwang.mydatabase.backend.vm;

import com.colinwang.mydatabase.backend.tm.TransactionManagerImpl;

import java.util.HashMap;
import java.util.Map;

/**
 * vm对一个事务的抽象
 * 事务隔离级别：
 * 0: 读已提交 (READ COMMITTED)
 * 1: 可重复读 (REPEATABLE READ)
  */
public class Transaction {
    public long xid;
    public int level; // 事务隔离级别
    public Map<Long, Boolean> snapshot; // 快照 存储事务启动时所有活跃事务的ID
    public Exception err;
    public boolean autoAborted; // 标志事务是否因为异常或超时被自动中止

    public static Transaction newTransaction(long xid, int level, Map<Long, Transaction> active) {
        Transaction t = new Transaction();
        t.xid = xid;
        t.level = level;
        if (level != 0) {
            t.snapshot = new HashMap<>();
            for (long x : active.keySet()) {
                t.snapshot.put(x, true);
            }
        }
        return t;
    }

    public boolean isInSnapshot(long xid) {
        if(xid == TransactionManagerImpl.SUPER_XID) {
            return false;
        }
        return snapshot.containsKey(xid);
    }
}