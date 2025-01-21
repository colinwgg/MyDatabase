package com.colinwang.mydatabase.backend.dm;

import com.colinwang.mydatabase.backend.common.AbstractCache;
import com.colinwang.mydatabase.backend.dm.DataItem.DataItem;
import com.colinwang.mydatabase.backend.dm.logger.Logger;
import com.colinwang.mydatabase.backend.dm.page.Page;
import com.colinwang.mydatabase.backend.dm.pageCache.PageCache;
import com.colinwang.mydatabase.backend.dm.pageIndex.PageIndex;
import com.colinwang.mydatabase.backend.tm.TransactionManager;

public class DataManagerImpl extends AbstractCache<DataItem> implements DataManager {

    TransactionManager tm;
    PageCache pc;
    Logger logger;
    PageIndex pIndex;
    Page pageOne;

    public DataManagerImpl(int maxResource) {
        super(maxResource);
    }

    @Override
    protected DataItem getForCache(long key) throws Exception {
        return null;
    }

    @Override
    protected void releaseForCache(DataItem obj) {

    }

    public void logDataItem(long xid, DataItem di) {

    }

    public void releaseDataItem(DataItem di) {
        super.release(di.getUid());
    }
}