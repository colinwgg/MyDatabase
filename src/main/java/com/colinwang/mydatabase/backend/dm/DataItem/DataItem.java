package com.colinwang.mydatabase.backend.dm.DataItem;

import com.colinwang.mydatabase.backend.common.SubArray;

public interface DataItem {
    SubArray data();

    void before();
    void unBefore();
    void after(long xid);
    void release();

    long getUid();
    byte[] getOldRaw();
    SubArray getRaw();
    public static void setDataItemRawInvalid(byte[] raw) {

    }

}
