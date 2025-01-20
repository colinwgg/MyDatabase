package com.colinwang.mydatabase.backend.dm.DataItem;

import com.colinwang.mydatabase.backend.common.SubArray;

public interface DataItem {
    long getUid();
    byte[] getOldRaw();
    SubArray getRaw();
    public static void setDataItemRawInvalid(byte[] raw) {

    }

}
