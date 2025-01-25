package com.colinwang.mydatabase.backend.dm;

import com.colinwang.mydatabase.backend.common.AbstractCache;
import com.colinwang.mydatabase.backend.dm.DataItem.DataItem;
import com.colinwang.mydatabase.backend.dm.DataItem.DataItemImpl;
import com.colinwang.mydatabase.backend.dm.logger.Logger;
import com.colinwang.mydatabase.backend.dm.page.Page;
import com.colinwang.mydatabase.backend.dm.page.PageOne;
import com.colinwang.mydatabase.backend.dm.page.PageX;
import com.colinwang.mydatabase.backend.dm.pageCache.PageCache;
import com.colinwang.mydatabase.backend.dm.pageIndex.PageIndex;
import com.colinwang.mydatabase.backend.dm.pageIndex.PageInfo;
import com.colinwang.mydatabase.backend.tm.TransactionManager;
import com.colinwang.mydatabase.backend.utils.Panic;
import com.colinwang.mydatabase.backend.utils.Types;
import com.colinwang.mydatabase.common.Error;

/**
 * DM层对外提供方法的类 & DataItem对象的缓存
 */
public class DataManagerImpl extends AbstractCache<DataItem> implements DataManager {

    TransactionManager tm;
    PageCache pc;
    Logger logger;
    PageIndex pIndex;
    Page pageOne;

    public DataManagerImpl(PageCache pc, Logger logger, TransactionManager tm) {
        super(0);
        this.pc = pc;
        this.logger = logger;
        this.tm = tm;
        this.pIndex = new PageIndex();
    }

    @Override
    protected DataItem getForCache(long uid) throws Exception {
        short offset = (short) (uid & ((1L << 16) - 1));
        uid >>>= 32;
        int pgno = (int) (uid & ((1L << 32) - 1));
        Page pg = pc.getPage(pgno);
        return DataItem.parseDataItem(pg, offset, this);
    }

    @Override
    protected void releaseForCache(DataItem di) {
        di.page().release();
    }

    // 为xid生成update日志
    public void logDataItem(long xid, DataItem di) {
        byte[] log = Recover.updateLog(xid, di);
        logger.log(log);
    }

    public void releaseDataItem(DataItem di) {
        super.release(di.getUid());
    }

    @Override
    public DataItem read(long uid) throws Exception {
        DataItemImpl di = (DataItemImpl) super.get(uid);
        if (!di.isValid()) {
            di.release();
            return null;
        }
        return di;
    }

    @Override
    public long insert(long xid, byte[] data) throws Exception {
        byte[] raw = DataItem.wrapDataItemRaw(data);
        if (raw.length > PageX.MAX_FREE_SPACE) {
            throw Error.DataTooLargeException;
        }

        PageInfo pi = null;
        for (int i = 0; i < 5; i++) {
            // 尝试从页面索引获取可用页面
            pi = pIndex.select(raw.length);
            if (pi != null) {
                break;
            } else {
                // 获取不到则生成新的空页面并添加到页面索引
                int newPgno = pc.newPage(PageX.iniRaw());
                pIndex.add(newPgno, PageX.MAX_FREE_SPACE);
            }
        }
        if (pi == null) {
            throw Error.DatabaseBusyException;
        }

        Page pg = null;
        int defaultFreeSpace = 0;
        try {
            pg = pc.getPage(pi.pgno);
            // 先写入insert日志
            byte[] log = Recover.insertLog(xid, pg, raw);
            logger.log(log);
            // 接下来插入日志并获取offset
            short offset = PageX.insert(pg, raw);

            pg.release();
            return Types.addressToUid(pi.pgno, offset);
        } finally {
            if (pg != null) {
                // 最后需要将pg重新插入页面索引
                pIndex.add(pi.pgno, PageX.getFreeSpace(pg));
            } else {
                pIndex.add(pi.pgno, defaultFreeSpace);
            }
        }
    }

    @Override
    public void close() {
        super.close();
        logger.close();

        PageOne.setVcClose(pageOne);
        pageOne.release();
        pc.close();
    }

    // 创建文件时初始化PageOne
    void initPageOne() {
        int pgno = pc.newPage(PageOne.InitRaw());
        assert pgno == 1;
        try {
            pageOne = pc.getPage(pgno);
        } catch (Exception e) {
            Panic.panic(e);
        }
        pc.flushPage(pageOne);
    }

    // 在打开已有文件时时读入PageOne，并验证正确性
    boolean loadCheckPageOne() {
        try {
            pageOne = pc.getPage(1);
        } catch (Exception e) {
            Panic.panic(e);
        }
        return PageOne.checkVc(pageOne);
    }

    // 初始化pageIndex
    void fillPageIndex() {
        int pageNumber = pc.getPageNumber();
        for (int i = 2; i <= pageNumber; i++) {
            Page pg = null;
            try {
                pg = pc.getPage(i);
            } catch (Exception e) {
                Panic.panic(e);
            }
            pIndex.add(pg.getPageNumber(), PageX.getFreeSpace(pg));
            pg.release();
        }
    }
}