package top.guoziyang.mydb.backend.dm.pageIndex;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import top.guoziyang.mydb.backend.dm.pageCache.PageCache;

public class PageIndex {
    // 将一页划成40个区间
    private static final int INTERVALS_NO = 40;
    // 每个区间的大小
    private static final int THRESHOLD = PageCache.PAGE_SIZE / INTERVALS_NO;
    private final Lock lock;
    private final List<PageInfo>[] lists;

    @SuppressWarnings("unchecked")
    public PageIndex() {
        lock = new ReentrantLock();
        lists = new List[INTERVALS_NO + 1];
        // 给每个区间new一个对应的list
        for (int i = 0; i < INTERVALS_NO + 1; i++) {
            lists[i] = new ArrayList<>();
        }
    }

    /*
        大概意思应该就是空闲空间为几个区块单位
        拥有相同区块单位数但数据页会被放进同一个list中
        这就对上了博客中的，从list中任意取一个页都有所需的剩余空间
     */
    public void add(int pageNo, int freeSpace) {
        lock.lock();
        try {
            int number = freeSpace / THRESHOLD;
            lists[number].add(new PageInfo(pageNo, freeSpace));
        } finally {
            lock.unlock();
        }
    }

    public PageInfo select(int spaceSize) {
        lock.lock();
        try {
            // 根据所需空间找出有对应空闲空间的页面文件列表
            int number = spaceSize / THRESHOLD;
            // 这个可能指第0页为保留页
            if (number < INTERVALS_NO) number++;
            // 这个循环应该是指如果没有这样的页，就去找含有更大空间的页
            while (number <= INTERVALS_NO) {
                if (lists[number].size() == 0) {
                    number++;
                    continue;
                }
                // 有这样空闲空间的页随便取一个返回就行，不过用了这个页就得把它从空闲列表中取出
                return lists[number].remove(0);
            }
            return null;
        } finally {
            lock.unlock();
        }
    }

}
