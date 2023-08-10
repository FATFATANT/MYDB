package top.guoziyang.mydb.backend.common;

import java.util.HashMap;
import java.util.Set;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import top.guoziyang.mydb.common.Error;

/**
 * AbstractCache 实现了一个引用计数策略的缓存
 */
public abstract class AbstractCache<T> {
    private final HashMap<Long, T> cache;                     // 实际缓存的数据
    private final HashMap<Long, Integer> references;          // 元素的引用个数
    private final HashMap<Long, Boolean> getting;             // 正在获取某资源的线程
    private final int maxResource;                            // 缓存的最大缓存资源数
    private int count = 0;                              // 缓存中元素的个数
    private final Lock lock;

    public AbstractCache(int maxResource) {
        this.maxResource = maxResource;
        cache = new HashMap<>();
        references = new HashMap<>();
        getting = new HashMap<>();
        lock = new ReentrantLock();
    }

    protected T get(long key) throws Exception {
        while (true) {
            lock.lock();
            // 由于某个线程获取这个资源后就将其赋值为true，有点奇怪，如果只能给一个线程获取，那么引用计数的意义是啥
            if (getting.containsKey(key)) {
                // 请求的资源正在被其他线程获取
                lock.unlock();
                try {
                    Thread.sleep(1);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                    continue;
                }
                continue;
            }
            // 当前缓存资源可用，将其引用计数加1然后返回资源
            if (cache.containsKey(key)) {
                // 资源在缓存中，直接返回
                T obj = cache.get(key);
                references.put(key, references.get(key) + 1);
                lock.unlock();
                return obj;
            }
            // 缓存满了就直接报错了
            if (maxResource > 0 && count == maxResource) {
                lock.unlock();
                throw Error.CacheFullException;
            }
            // 代码走到这里说明缓存中没有，缓存空间还有剩余可以加入，此时就增加缓存中记录数
            count++;
            // 缓存中没有但是先把这个标记为已使用，我猜是为了避免多个线程同时去数据源获取资源
            getting.put(key, true);
            lock.unlock();
            break;
        }
        // 这个while循环外面就不是从缓存中获取，而是从数据源获取
        T obj;
        try {
            obj = getForCache(key);
        } catch (Exception e) {
            // 相当于发生异常回滚
            lock.lock();
            count--;
            getting.remove(key);
            lock.unlock();
            throw e;
        }

        lock.lock();
        // 这个remove就很奇怪了，不是很懂为啥
        getting.remove(key);
        cache.put(key, obj);
        references.put(key, 1);
        lock.unlock();

        return obj;
    }

    /**
     * 强行释放一个缓存
     */
    protected void release(long key) {
        lock.lock();
        try {
            // 释放一个缓存并不是直接将这个元素删除，是先降低引用计数
            // 当引用计数为0时才真正将这个元素删除
            int ref = references.get(key) - 1;
            if (ref == 0) {
                T obj = cache.get(key);
                releaseForCache(obj);
                references.remove(key);
                cache.remove(key);
                count--;
            } else {
                references.put(key, ref);
            }
        } finally {
            lock.unlock();
        }
    }

    /**
     * 关闭缓存，写回所有资源
     */
    protected void close() {
        // 这个意思大概是缓存中被去除的元素会被写会硬盘
        lock.lock();
        try {
            Set<Long> keys = cache.keySet();
            for (long key : keys) {
                T obj = cache.get(key);
                releaseForCache(obj);
                references.remove(key);
                cache.remove(key);
            }
        } finally {
            lock.unlock();
        }
    }


    /**
     * 当资源不在缓存时的获取行为
     */
    protected abstract T getForCache(long key) throws Exception;

    /**
     * 当资源被驱逐时的写回行为
     */
    protected abstract void releaseForCache(T obj);
}
