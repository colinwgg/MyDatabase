package com.colinwang.mydatabase.backend.dm.DataItem;

import com.colinwang.mydatabase.backend.common.SubArray;
import com.colinwang.mydatabase.backend.dm.DataManagerImpl;
import com.colinwang.mydatabase.backend.dm.page.Page;
import com.colinwang.mydatabase.backend.utils.Parser;
import com.colinwang.mydatabase.backend.utils.Types;
import com.google.common.primitives.Bytes;

import java.util.Arrays;

public interface DataItem {
    SubArray data();

    void before();
    void unBefore();
    void after(long xid);
    void release();

    void lock();
    void unlock();
    void rLock();
    void rUnLock();

    Page page();
    long getUid();
    byte[] getOldRaw();
    SubArray getRaw();

    // 将原始数据封装为DataItem格式
    public static byte[] wrapDataItemRaw(byte[] raw) {
        byte[] validFlag = new byte[1];
        byte[] dataSize = Parser.short2Byte((short) raw.length); // dataSize为2字节
        return Bytes.concat(validFlag, dataSize, raw);
    }

    // 从页面的offset处解析处dataItem
    public static DataItem parseDataItem(Page pg, short offset, DataManagerImpl dm) {
        byte[] raw = pg.getData();
        short size = Parser.parseShort(Arrays.copyOfRange(raw, offset + DataItemImpl.OF_SIZE, offset + DataItemImpl.OF_DATA));
        short length = (short) (size + DataItemImpl.OF_DATA);
        long uid = Types.addressToUid(pg.getPageNumber(), offset);
        return new DataItemImpl(new SubArray(raw, offset, offset + length), new byte[length], pg, uid, dm);
    }

    public static void setDataItemRawInvalid(byte[] raw) {
        raw[DataItemImpl.OF_VALID] = (byte) 1;
    }
}
