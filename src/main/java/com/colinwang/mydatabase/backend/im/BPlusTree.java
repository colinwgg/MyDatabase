package com.colinwang.mydatabase.backend.im;

import com.colinwang.mydatabase.backend.dm.DataItem.DataItem;
import com.colinwang.mydatabase.backend.dm.DataManager;

import java.util.concurrent.locks.Lock;

public class BPlusTree {
    DataManager dm;
    long bootUid;
    DataItem bootDataItem;
    Lock bootLock;
}