package top.guoziyang.mydb.backend.vm;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import top.guoziyang.mydb.common.Error;

/**
 * 维护了一个依赖等待图，以进行死锁检测
 */
public class LockTable {

    private final Map<Long, List<Long>> x2u;  // 某个XID已经获得的资源的UID列表
    private final Map<Long, Long> u2x;        // UID被某个XID持有
    private final Map<Long, List<Long>> wait; // 正在等待UID的XID列表
    private final Map<Long, Lock> waitLock;   // 正在等待资源的XID的锁
    private final Map<Long, Long> waitU;      // XID正在等待的UID
    private final Lock lock;

    public LockTable() {
        x2u = new HashMap<>();
        u2x = new HashMap<>();
        wait = new HashMap<>();
        waitLock = new HashMap<>();
        waitU = new HashMap<>();
        lock = new ReentrantLock();
    }

    /*
        此处的返回值，如果不需要等待则返回null，否则返回锁对象
        调用add，如果需要等待的话，会返回一个上了锁的 Lock 对象
        调用方在获取到该对象时，需要尝试获取该对象的锁，由此实现阻塞线程的目的
        检测到死锁则会抛出异常
     */
    public Lock add(long xid, long uid) throws Exception {
        lock.lock();
        try {
            // 事务已经持有该资源
            if (isInList(x2u, xid, uid)) {
                return null;
            }
            // 资源处于空闲状态，就让事务获取资源，建立事务和资源的映射关系
            if (u2x.containsKey(uid)) {
                u2x.put(uid, xid);
                putIntoList(x2u, xid, uid);
                return null;
            }
            // 资源正被别的事务使用，记录当前事务对资源的请求
            waitU.put(xid, uid);
            // 注意这里是头插法，大概用来表示最近请求uid资源的事务
            putIntoList(wait, uid, xid);
            // 如果检测到了死锁
            if (hasDeadLock()) {
                // 撤销该事务对资源的请求
                waitU.remove(xid);
                // 去除等待uid的列表中的事务xid对应项
                removeFromList(wait, uid, xid);
                throw Error.DeadlockException;
            }
            Lock l = new ReentrantLock();
            l.lock();
            waitLock.put(xid, l);
            return l;

        } finally {
            lock.unlock();
        }
    }

    /*
        在事务提交或者撤销后被调用
     */
    public void remove(long xid) {
        lock.lock();
        try {
            // 事务持有的所有资源
            List<Long> l = x2u.get(xid);
            if (l != null) {
                while (l.size() > 0) {
                    Long uid = l.remove(0);
                    // 释放的资源可以给正在请求的事务
                    selectNewXID(uid);
                }
            }
            waitU.remove(xid);
            x2u.remove(xid);
            waitLock.remove(xid);

        } finally {
            lock.unlock();
        }
    }

    // 从等待队列中选择一个xid来占用uid
    private void selectNewXID(long uid) {
        u2x.remove(uid);
        // 获取对该资源的等待队列
        List<Long> l = wait.get(uid);
        if (l == null) return;
        assert l.size() > 0;

        while (l.size() > 0) {
            // 将最近请求这个资源的事务取出
            long xid = l.remove(0);
            if (waitLock.containsKey(xid)) {
                // 构造资源与事务的映射关系
                u2x.put(uid, xid);
                Lock lo = waitLock.remove(xid);
                // 去除事务对该资源的等待
                waitU.remove(xid);
                lo.unlock();
                break;
            }
        }

        if (l.size() == 0) wait.remove(uid);
    }

    private Map<Long, Integer> xidStamp;
    private int stamp;

    private boolean hasDeadLock() {
        xidStamp = new HashMap<>();
        stamp = 1;
        // 遍历已经持有资源的事务
        for (long xid : x2u.keySet()) {
            Integer s = xidStamp.get(xid);
            if (s != null && s > 0) {
                continue;
            }
            stamp++;
            // 以该事务为根结点进行遍历
            if (dfs(xid)) {
                return true;
            }
        }
        return false;
    }

    /*
        正如博客所说，如果在一次深度优先遍历中能遍历到之前访问过的结点
        那就说明发生了死锁
     */
    private boolean dfs(long xid) {
        Integer stp = xidStamp.get(xid);
        if (stp != null && stp == stamp) {
            return true;
        }
        if (stp != null && stp < stamp) {
            return false;
        }
        xidStamp.put(xid, stamp);
        // 取出当前事务请求的资源
        Long uid = waitU.get(xid);
        if (uid == null) return false;
        // 取出正持有当前请求的资源的事务
        Long x = u2x.get(uid);
        assert x != null;
        //
        return dfs(x);
    }

    private void removeFromList(Map<Long, List<Long>> listMap, long id0, long id1) {
        List<Long> l = listMap.get(id0);
        if (l == null) return;
        Iterator<Long> i = l.iterator();
        // 去掉id0对应的list中的id1项
        while (i.hasNext()) {
            long e = i.next();
            if (e == id1) {
                i.remove();
                break;
            }
        }
        // 如果id0的list去掉这一项就空了那就把map中的id0直接删了
        if (l.size() == 0) {
            listMap.remove(id0);
        }
    }

    // 由于删除时也是取头部的，所以我感觉它这个公平锁有问题，这就变成先响应给最近需要的事务
    // 但这个逻辑也是合理的，不过不能用公平锁来解释
    private void putIntoList(Map<Long, List<Long>> listMap, long id0, long id1) {
        if (!listMap.containsKey(id0)) {
            listMap.put(id1, new ArrayList<>());
        }
        // 头插法
        listMap.get(id0).add(0, id1);
    }

    private boolean isInList(Map<Long, List<Long>> listMap, long xid, long uid) {
        // 取出xid对应事务持有的所有uid
        List<Long> l = listMap.get(xid);
        if (l == null) return false;
        for (long e : l) {
            if (e == uid) {
                return true;
            }
        }
        return false;
    }

}
