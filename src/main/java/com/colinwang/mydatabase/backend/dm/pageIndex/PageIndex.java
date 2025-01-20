package com.colinwang.mydatabase.backend.dm.pageIndex;

import com.colinwang.mydatabase.backend.dm.page.Page;
import com.colinwang.mydatabase.backend.dm.pageCache.PageCache;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class PageIndex {
    private static final int INTERVALS = 40;
    private static final int THRESHOLD = PageCache.PAGE_SIZE / INTERVALS;

    private Lock lock;
    private List<PageInfo>[] lists;

    @SuppressWarnings("unchecked")
    public PageIndex() {
        lock = new ReentrantLock();
        lists = new List[INTERVALS + 1];
        for (int i = 0; i < INTERVALS + 1; i++) {
            lists[i] = new ArrayList<>();
        }
    }

    public PageInfo select(int spaceSize) {
        lock.lock();
        try {
            int num = spaceSize / THRESHOLD;
            if (num < INTERVALS) num++;
            while (num <= INTERVALS) {
                if (lists[num].isEmpty()) {
                    num++;
                    continue;
                }
                return lists[num].remove(0);
            }
            return null;
        } finally {
            lock.unlock();
        }
    }

    public void add(int pgno, int freeSpace) {
        int num = freeSpace / THRESHOLD;
        lists[num].add(new PageInfo(pgno, freeSpace));
    }
}
