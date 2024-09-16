package com.colinwang.mydatabase.backend.dm.page;

import com.colinwang.mydatabase.backend.dm.pageCache.PageCache;
import com.colinwang.mydatabase.backend.utils.RandomUtil;

import java.lang.reflect.Array;
import java.util.Arrays;

/**
 * 特殊管理第一页
 * ValidCheck
 * db启动时给100~107字节处填入一个随机字节，db关闭时将其拷贝到108~115字节
 * 用于判断上一次数据库是否正常关闭
 */
public class PageOne {
    private static final int OFFSET_VC = 100;
    private static final int LEN_VC = 8;

    public static byte[] InitRaw() {
        byte[] raw = new byte[PageCache.PAGE_SIZE];
        setVcOpen(raw);
        return raw;
    }

    public static void setVcOpen(Page pg) {
        pg.setDirty(true);
        setVcOpen(pg.getData());
    }

    private static void setVcOpen(byte[] raw) {
        System.arraycopy(RandomUtil.randomBytes(LEN_VC), 0, raw, OFFSET_VC, LEN_VC);
    }

    public static void setVcClose(Page pg) {
        pg.setDirty(true);
        setVcClose(pg.getData());
    }

    private static void setVcClose(byte[] raw) {
        System.arraycopy(raw, OFFSET_VC, raw, OFFSET_VC + LEN_VC, LEN_VC);
    }

    public static boolean checkVc(Page pg) {
        return checkVc(pg.getData());
    }

    private static boolean checkVc(byte[] raw) {
        return Arrays.equals(Arrays.copyOfRange(raw, OFFSET_VC, OFFSET_VC + LEN_VC), Arrays.copyOfRange(raw, OFFSET_VC + LEN_VC, OFFSET_VC + LEN_VC * 2));
    }
}