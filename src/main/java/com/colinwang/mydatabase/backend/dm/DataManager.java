package com.colinwang.mydatabase.backend.dm;

import com.colinwang.mydatabase.backend.dm.DataItem.DataItem;
import com.colinwang.mydatabase.backend.dm.DataItem.DataItemImpl;
import com.colinwang.mydatabase.backend.dm.logger.Logger;
import com.colinwang.mydatabase.backend.dm.page.PageOne;
import com.colinwang.mydatabase.backend.dm.pageCache.PageCache;
import com.colinwang.mydatabase.backend.dm.pageCache.PageCacheImpl;
import com.colinwang.mydatabase.backend.tm.TransactionManager;

public interface DataManager {
    DataItem read(long uid) throws Exception;
    long insert(long xid, byte[] data) throws Exception;
    void close();

    public static DataManagerImpl create(String path, long mem, TransactionManager tm) {
        PageCache pc = PageCache.create(path, mem);
        Logger log = Logger.create(path);
        DataManagerImpl dm = new DataManagerImpl(pc, log, tm);
        dm.initPageOne();
        return dm;
    }

    public static DataManagerImpl open(String path, long mem, TransactionManager tm) {
        PageCacheImpl pc = PageCache.open(path, mem);
        Logger log = Logger.open(path);
        DataManagerImpl dm = new DataManagerImpl(pc, log, tm);
        if (!dm.loadCheckPageOne()) {
            Recover.recover(tm, log, pc);
        }
        dm.fillPageIndex();
        PageOne.setVcOpen(dm.pageOne);
        dm.pc.flushPage(dm.pageOne);
        return dm;
    }
}