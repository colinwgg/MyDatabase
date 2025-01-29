package com.colinwang.mydatabase.backend.vm;

import com.colinwang.mydatabase.backend.common.AbstractCache;
import com.colinwang.mydatabase.backend.dm.DataManager;
import com.colinwang.mydatabase.backend.tm.TransactionManager;
import com.colinwang.mydatabase.backend.tm.TransactionManagerImpl;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class VersionManagerImpl extends AbstractCache<Entry> implements VersionManager {

    TransactionManager tm;
    DataManager dm;
    Map<Long, Transaction> activeTransaction;
    Lock lock;
    LockTable lt;

    public VersionManagerImpl(TransactionManager tm, DataManager dm) {
        super(0);
        this.tm = tm;
        this.dm = dm;
        this.activeTransaction = new HashMap<>();
        activeTransaction.put(TransactionManagerImpl.SUPER_XID, Transaction.newTransaction(TransactionManagerImpl.SUPER_XID, 0, null));
        this.lock = new ReentrantLock();
        this.lt = new LockTable();
    }

    @Override
    protected Entry getForCache(long key) throws Exception {
        return null;
    }

    @Override
    protected void releaseForCache(Entry obj) {

    }
}