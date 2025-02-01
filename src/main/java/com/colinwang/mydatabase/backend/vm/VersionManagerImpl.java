package com.colinwang.mydatabase.backend.vm;

import com.colinwang.mydatabase.backend.common.AbstractCache;
import com.colinwang.mydatabase.backend.dm.DataManager;
import com.colinwang.mydatabase.backend.tm.TransactionManager;
import com.colinwang.mydatabase.backend.tm.TransactionManagerImpl;
import com.colinwang.mydatabase.common.Error;

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
    public byte[] read(long xid, long uid) throws Exception {
        lock.lock();
        Transaction t = activeTransaction.get(xid);
        lock.unlock();
        if (t.err != null) {
            throw t.err;
        }

        Entry entry = null;
        try {
            entry = super.get(uid);
        } catch (Exception e) {
            if (e == Error.NullEntryException) {
                return null; // 如果条目为空，返回null
            } else {
                throw e;
            }
        }

        try {
            if (Visibility.isVisible(tm, t, entry)) {
                return entry.data(); // 如果条目对事务可见，返回数据
            } else {
                return null;
            }
        } finally {
            entry.release();
        }
    }

    @Override
    public long insert(long xid, byte[] data) throws Exception {
        lock.lock();
        Transaction t = activeTransaction.get(xid);
        lock.unlock();
        if(t.err != null) {
            throw t.err;
        }

        byte[] raw = Entry.wrapEntryRaw(xid, data);
        return dm.insert(xid, raw);
    }

    @Override
    public boolean delete(long xid, long uid) throws Exception {
        lock.lock();
        Transaction t = activeTransaction.get(xid);
        lock.unlock();
        if(t.err != null) {
            throw t.err;
        }

        Entry entry = null;
        try {
            entry = super.get(uid);
        } catch (Exception e) {
            if (e == Error.NullEntryException) {
                return false;
            } else {
                throw e;
            }
        }

        try {
            if(!Visibility.isVisible(tm, t, entry)) {
                return false;
            }

            Lock lk = null;
            try {
                lk = lt.add(xid, uid); // 为当前事务获取锁
            } catch (Exception e) {
                t.err = Error.ConcurrentUpdateException;
                internAbort(xid, true); // 获取锁失败，回滚事务
                t.autoAborted = true;
                throw t.err;
            }
            if (lk != null) {
                lk.lock();
                lk.unlock();
            }

            if (entry.getXmax() == xid) {
                return false; // 如果事务已标记为删除，返回false
            }

            if(Visibility.isVersionSkip(tm, t, entry)) {
                t.err = Error.ConcurrentUpdateException;
                internAbort(xid, true);
                t.autoAborted = true;
                throw t.err;
            }

            entry.setXmax(xid); // 设置删除标记
            return true;
        } finally {
            entry.release();
        }
    }

    @Override
    public long begin(int level) {
        lock.lock();
        try {
            long xid = tm.begin();
            Transaction t = Transaction.newTransaction(xid, level, activeTransaction);
            activeTransaction.put(xid, t);
            return xid;
        } finally {
            lock.unlock();
        }
    }

    @Override
    public void commit(long xid) throws Exception {
        lock.lock();
        Transaction t = activeTransaction.get(xid);
        lock.unlock();
        if(t.err != null) {
            throw t.err;
        }

        lock.lock();
        activeTransaction.remove(xid);
        lock.unlock();

        lt.remove(xid);
        tm.commit(xid);
    }

    @Override
    public void abort(long xid) {
        internAbort(xid, false);
    }

    private void internAbort(long xid, boolean autoAborted) {
        lock.lock();
        Transaction t = activeTransaction.get(xid);
        if(!autoAborted) {
            activeTransaction.remove(xid); // 如果不是自动回滚，移除事务
        }
        lock.unlock();
        if(t.autoAborted) return; // 如果事务已自动回滚，不再处理
        lt.remove(xid);
        tm.abort(xid);
    }

    @Override
    protected Entry getForCache(long uid) throws Exception {
        Entry entry = Entry.loadEntry(this, uid);
        if (entry == null) {
            throw Error.NullEntryException;
        }
        return entry;
    }

    // 释放底层数据资源
    @Override
    protected void releaseForCache(Entry entry) {
        entry.remove();
    }

    // 解除缓存引用，释放逻辑资源
    public void releaseEntry(Entry entry) {
        super.release(entry.getUid());
    }
}