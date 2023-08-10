package top.guoziyang.mydb.backend.dm.pageCache;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import top.guoziyang.mydb.backend.common.AbstractCache;
import top.guoziyang.mydb.backend.dm.page.Page;
import top.guoziyang.mydb.backend.dm.page.PageImpl;
import top.guoziyang.mydb.backend.utils.Panic;
import top.guoziyang.mydb.common.Error;

public class PageCacheImpl extends AbstractCache<Page> implements PageCache {

    private static final int MEM_MIN_LIM = 10;
    public static final String DB_SUFFIX = ".db";
    private final RandomAccessFile file;
    private final FileChannel fc;
    private final Lock fileLock;
    private final AtomicInteger pageNumbers;

    PageCacheImpl(RandomAccessFile file, FileChannel fileChannel, int maxResource) {
        super(maxResource);
        if (maxResource < MEM_MIN_LIM) {
            Panic.panic(Error.MemTooSmallException);
        }
        long length = 0;
        try {
            length = file.length();
        } catch (IOException e) {
            Panic.panic(e);
        }
        this.file = file;
        this.fc = fileChannel;
        this.fileLock = new ReentrantLock();
        // 这个当前总页数是根据文件大小除以每页大小算出来的
        this.pageNumbers = new AtomicInteger((int) length / PAGE_SIZE);
    }

    public int newPage(byte[] initData) {
        int pageNo = pageNumbers.incrementAndGet();
        Page pg = new PageImpl(pageNo, initData, null);
        flush(pg);
        return pageNo;
    }

    public Page getPage(int pageNo) throws Exception {
        return get(pageNo);
    }

    /**
     * 根据pageNumber从数据库文件中读取页数据，并包裹成Page
     */
    @Override
    protected Page getForCache(long key) {
        /*
            整个逻辑大概就是根据页号计算出该页在文件对应字节数组的偏移量
            将指针移动到这个位置然后读取一页的数据，将这个数据包进PageImpl
         */
        int pageNo = (int) key;
        long offset = PageCacheImpl.pageOffset(pageNo);

        ByteBuffer buf = ByteBuffer.allocate(PAGE_SIZE);
        fileLock.lock();
        try {
            fc.position(offset);
            fc.read(buf);
        } catch (IOException e) {
            Panic.panic(e);
        }
        fileLock.unlock();
        return new PageImpl(pageNo, buf.array(), this);
    }

    @Override
    protected void releaseForCache(Page pg) {
        if (pg.isDirty()) {
            flush(pg);
            pg.setDirty(false);
        }
    }

    public void release(Page page) {
        release((long) page.getPageNumber());
    }

    public void flushPage(Page pg) {
        flush(pg);
    }

    private void flush(Page pg) {
        /*
            将数据从缓存写回磁盘也是类似的，不过这次是从封装好的Page对象里面
            取出页号算出偏移量然后将缓存中该页对应的字节数组覆盖磁盘文件所在位置
         */
        int pageNumber = pg.getPageNumber();
        long offset = pageOffset(pageNumber);
        fileLock.lock();
        try {
            ByteBuffer buf = ByteBuffer.wrap(pg.getData());
            fc.position(offset);
            fc.write(buf);
            fc.force(false);
        } catch (IOException e) {
            Panic.panic(e);
        } finally {
            fileLock.unlock();
        }
    }

    public void truncateByPageNo(int maxPageNo) {
        /*
            截断，字面意，算出该页的偏移量，直接将磁盘文件大小缩小到偏移量所在位置
            消灭磁盘文件那确实就是截断
         */
        long size = pageOffset(maxPageNo + 1);
        try {
            file.setLength(size);
        } catch (IOException e) {
            Panic.panic(e);
        }
        pageNumbers.set(maxPageNo);
    }

    @Override
    public void close() {
        super.close();
        try {
            fc.close();
            file.close();
        } catch (IOException e) {
            Panic.panic(e);
        }
    }

    public int getPageNumber() {
        return pageNumbers.intValue();
    }

    private static long pageOffset(int pgno) {
        return (long) (pgno - 1) * PAGE_SIZE;
    }

}
