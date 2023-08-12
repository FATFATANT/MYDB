package top.guoziyang.mydb.backend.vm;

import top.guoziyang.mydb.backend.tm.TransactionManager;

public class Visibility {
    /*
        解决版本跳跃问题
     */
    public static boolean isVersionSkip(TransactionManager tm, Transaction t, Entry e) {
        long xmax = e.getXmax();
        if (t.level == 0) {
            return false;
        } else {
            // 晚于当前事务的事务是不可见的，早于当前事务的事务如果没提交也是不可见的
            return tm.isCommitted(xmax) && (xmax > t.xid || t.isInSnapshot(xmax));
        }
    }

    public static boolean isVisible(TransactionManager tm, Transaction t, Entry e) {
        if (t.level == 0) {
            return readCommitted(tm, t, e);
        } else {
            return repeatableRead(tm, t, e);
        }
    }

    // 检查版本是否对事务t可见
    private static boolean readCommitted(TransactionManager tm, Transaction t, Entry e) {
        long xid = t.xid;
        long xmin = e.getXmin();
        long xmax = e.getXmax();
        // 由当前事务创建的版本且未被删除
        if (xmin == xid && xmax == 0) return true;
        // 当前版本由一个已提交的事务创建
        if (tm.isCommitted(xmin)) {
            // 该版本尚未被删除
            if (xmax == 0) return true;
            // 删除该版本的事务还没提交
            if (xmax != xid) {
                return !tm.isCommitted(xmax);
            }
        }
        return false;
    }

    private static boolean repeatableRead(TransactionManager tm, Transaction t, Entry e) {
        long xid = t.xid;
        long xmin = e.getXmin();
        long xmax = e.getXmax();
        // 当前版本由当前事务创建且未被删除
        if (xmin == xid && xmax == 0) return true;
        // 当前版本由一个已提交的事务创建且那个事务早于当前事务，不过也奇怪，为啥要加上最后一个条件，已提交的怎么可能活跃呢
        if (tm.isCommitted(xmin) && xmin < xid && !t.isInSnapshot(xmin)) {
            // 还得事务尚未被删除
            if (xmax == 0) return true;
            if (xmax != xid) {
                // 由其他事务删除但是该事务尚未提交||这个事务在Ti开始之后才开始｜｜这个事务在Ti开始前还未提交
                return !tm.isCommitted(xmax) || xmax > xid || t.isInSnapshot(xmax);
            }
        }
        return false;
    }

}
