package com.colinwang.mydatabase.backend.dm.page;

import com.colinwang.mydatabase.backend.dm.pageCache.PageCache;
import com.colinwang.mydatabase.backend.utils.Parser;

import java.util.Arrays;

/**
 * PageX管理普通页
 * 普通页结构
 * [FreeSpaceOffset] [Data]
 * FreeSpaceOffset: 2字节 空闲位置开始偏移
 */
public class PageX {
    private static final short OFFSET_FREE = 0;
    private static final short OFFSET_DATA = 2;
    public static final int MAX_FREE_SPACE = PageCache.PAGE_SIZE - OFFSET_DATA;

    public static byte[] iniRaw() {
        byte[] raw = new byte[PageCache.PAGE_SIZE];
        setFSO(raw, OFFSET_DATA);
        return raw;
    }

    private static void setFSO(byte[] raw, short offset) {
        System.arraycopy(Parser.short2Byte(OFFSET_DATA), 0, raw, OFFSET_FREE, OFFSET_DATA);
    }

    public static short getFSO(Page pg) {
        return getFSO(pg.getData());
    }

    private static short getFSO(byte[] raw) {
        return Parser.parseShort(Arrays.copyOfRange(raw, 0, 2));
    }

    // 将raw（数据）插入page，返回插入位置offset
    public static short insert(Page pg, byte[] raw) {
        pg.setDirty(true);
        short fso = getFSO(pg.getData());
        System.arraycopy(raw, 0, pg.getData(), fso, raw.length);
        setFSO(pg.getData(), (short)(fso + raw.length));
        return fso;
    }

    // 获取页面的空闲空间大小
    public static int getFreeSpace(Page pg) {
        return PageCache.PAGE_SIZE - (int)getFSO(pg.getData());
    }

    // 将raw插入pg中的offset位置，并将pg的offset设置为较大的offset
    public static void recoverInsert(Page pg, byte[] raw, short offset) {
        pg.setDirty(true);
        System.arraycopy(raw, 0, pg.getData(), offset, raw.length);
        short rawFSO = getFSO(pg.getData());
        if (offset + raw.length > rawFSO) {
            setFSO(pg.getData(), (short)(offset + raw.length));
        }
    }

    // 将raw插入pg中的offset位置，不更新update
    public static void recoverUpdate(Page pg, byte[] raw, short offset) {
        pg.setDirty(true);
        System.arraycopy(raw, 0, pg.getData(), offset, raw.length);
    }
}