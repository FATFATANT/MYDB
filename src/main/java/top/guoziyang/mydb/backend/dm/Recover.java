package top.guoziyang.mydb.backend.dm;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import com.google.common.primitives.Bytes;

import top.guoziyang.mydb.backend.common.SubArray;
import top.guoziyang.mydb.backend.dm.dataItem.DataItem;
import top.guoziyang.mydb.backend.dm.logger.Logger;
import top.guoziyang.mydb.backend.dm.page.Page;
import top.guoziyang.mydb.backend.dm.page.PageX;
import top.guoziyang.mydb.backend.dm.pageCache.PageCache;
import top.guoziyang.mydb.backend.tm.TransactionManager;
import top.guoziyang.mydb.backend.utils.Panic;
import top.guoziyang.mydb.backend.utils.Parser;

public class Recover {

    private static final byte LOG_TYPE_INSERT = 0;
    private static final byte LOG_TYPE_UPDATE = 1;
    private static final int REDO = 0;
    private static final int UNDO = 1;

    static class InsertLogInfo {
        long xid;
        int pageNo;
        short offset;
        byte[] raw;
    }

    // 不过这里并没有写UID，此处应该是用offset来表示了
    static class UpdateLogInfo {
        long xid;
        int pageNo;
        short offset;
        byte[] oldRaw;
        byte[] newRaw;
    }

    public static void recover(TransactionManager tm, Logger lg, PageCache pc) {
        System.out.println("Recovering...");
        // 指针回滚到校验和之后
        lg.rewind();
        int maxPgno = 0;
        // 这个循环目前看来好像只是去掉了最后一页
        while (true) {
            // 取出日志中的Data部分
            byte[] log = lg.next();
            if (log == null) break;
            int pageNo;
            // Data部分第1个字节指明该条日志属于什么类型
            if (isInsertLog(log)) {
                InsertLogInfo li = parseInsertLog(log);
                pageNo = li.pageNo;
            } else {
                UpdateLogInfo li = parseUpdateLog(log);
                pageNo = li.pageNo;
            }
            // 应是新开页了
            if (pageNo > maxPgno) {
                maxPgno = pageNo;
            }
        }
        if (maxPgno == 0) {
            maxPgno = 1;
        }
        // 截断最后一个可能存在的不完整页
        pc.truncateByPageNo(maxPgno);
        System.out.println("Truncate to " + maxPgno + " pages.");

        redoTransactions(tm, lg, pc);
        System.out.println("Redo Transactions Over.");

        undoTransactions(tm, lg, pc);
        System.out.println("Undo Transactions Over.");

        System.out.println("Recovery Over.");
    }

    /*
        这边的redo和undo看起来是有些重复的代码
     */
    private static void redoTransactions(TransactionManager tm, Logger lg, PageCache pc) {
        lg.rewind();
        while (true) {
            byte[] log = lg.next();
            if (log == null) break;
            if (isInsertLog(log)) {
                InsertLogInfo li = parseInsertLog(log);
                long xid = li.xid;
                // 查找该日志的事务ID对应的事务记录的状态
                if (!tm.isActive(xid)) {
                    doInsertLog(pc, log, REDO);
                }
            } else {
                UpdateLogInfo xi = parseUpdateLog(log);
                long xid = xi.xid;
                if (!tm.isActive(xid)) {
                    doUpdateLog(pc, log, REDO);
                }
            }
        }
    }

    private static void undoTransactions(TransactionManager tm, Logger lg, PageCache pc) {
        Map<Long, List<byte[]>> logCache = new HashMap<>();
        lg.rewind();
        while (true) {
            byte[] log = lg.next();
            if (log == null) break;
            if (isInsertLog(log)) {
                InsertLogInfo li = parseInsertLog(log);
                long xid = li.xid;
                // undo的事件就是没提交或者撤销的事务，此处就是以事务编号为键，日志数据列表为值
                if (tm.isActive(xid)) {
                    if (!logCache.containsKey(xid)) {
                        logCache.put(xid, new ArrayList<>());
                    }
                    logCache.get(xid).add(log);
                }
            } else {
                UpdateLogInfo xi = parseUpdateLog(log);
                long xid = xi.xid;
                if (tm.isActive(xid)) {
                    if (!logCache.containsKey(xid)) {
                        logCache.put(xid, new ArrayList<>());
                    }
                    logCache.get(xid).add(log);
                }
            }
        }
        /*
            对所有active log进行倒序undo
            此处就是遍历每一个事务，倒序取出其日志，然后执行
         */
        for (Entry<Long, List<byte[]>> entry : logCache.entrySet()) {
            List<byte[]> logs = entry.getValue();
            for (int i = logs.size() - 1; i >= 0; i--) {
                byte[] log = logs.get(i);
                if (isInsertLog(log)) {
                    doInsertLog(pc, log, UNDO);
                } else {
                    doUpdateLog(pc, log, UNDO);
                }
            }
            // 撤销所有操作后将该事务标记为撤销事务
            tm.abort(entry.getKey());
        }
    }

    private static boolean isInsertLog(byte[] log) {
        return log[0] == LOG_TYPE_INSERT;
    }

    /*
        更新日志的数据部分的组成结构
        [LogType] [XID] [UID] [OldRaw] [NewRaw]
        1字节      8字节  8字节  8字节
     */
    private static final int OF_TYPE = 0;
    private static final int OF_XID = OF_TYPE + 1;
    private static final int OF_UPDATE_UID = OF_XID + 8;
    private static final int OF_UPDATE_RAW = OF_UPDATE_UID + 8;

    public static byte[] updateLog(long xid, DataItem di) {
        byte[] logType = {LOG_TYPE_UPDATE};
        byte[] xidRaw = Parser.long2Byte(xid);
        byte[] uidRaw = Parser.long2Byte(di.getUid());
        byte[] oldRaw = di.getOldRaw();
        SubArray raw = di.getRaw();
        byte[] newRaw = Arrays.copyOfRange(raw.raw, raw.start, raw.end);
        return Bytes.concat(logType, xidRaw, uidRaw, oldRaw, newRaw);
    }

    private static UpdateLogInfo parseUpdateLog(byte[] log) {
        UpdateLogInfo li = new UpdateLogInfo();
        // 8个字节
        li.xid = Parser.parseLong(Arrays.copyOfRange(log, OF_XID, OF_UPDATE_UID));
        // 8个字节，Data中是有uid的，不过这里给他提前计算好这个uid对应的偏移量和页号
        long uid = Parser.parseLong(Arrays.copyOfRange(log, OF_UPDATE_UID, OF_UPDATE_RAW));
        li.offset = (short) (uid & ((1L << 16) - 1));
        uid >>>= 32;
        li.pageNo = (int) (uid & ((1L << 32) - 1));
        // 说明新旧数据的存储大小是一样的，前半段是旧的后半段是新的
        int length = (log.length - OF_UPDATE_RAW) / 2;
        li.oldRaw = Arrays.copyOfRange(log, OF_UPDATE_RAW, OF_UPDATE_RAW + length);
        li.newRaw = Arrays.copyOfRange(log, OF_UPDATE_RAW + length, OF_UPDATE_RAW + length * 2);
        return li;
    }

    private static void doUpdateLog(PageCache pc, byte[] log, int flag) {
        int pageNo;
        short offset;
        byte[] raw;
        // 更新的REDO和UNDO只是raw值不同，就是从日志中选取数据更新到磁盘
        if (flag == REDO) {
            UpdateLogInfo xi = parseUpdateLog(log);
            pageNo = xi.pageNo;
            offset = xi.offset;
            raw = xi.newRaw;
        } else {
            UpdateLogInfo xi = parseUpdateLog(log);
            pageNo = xi.pageNo;
            offset = xi.offset;
            raw = xi.oldRaw;
        }
        Page pg = null;
        try {
            pg = pc.getPage(pageNo);
        } catch (Exception e) {
            Panic.panic(e);
        }
        try {
            assert pg != null;
            PageX.recoverUpdate(pg, raw, offset);
        } finally {
            assert pg != null;
            pg.release();
        }
    }

    /*
        插入日志的组成结构
       [LogType] [XID] [PageNo] [Offset] [Raw]
       1字节      8字节  4字节     2字节
     */
    private static final int OF_INSERT_PAGE_NO = OF_XID + 8;
    private static final int OF_INSERT_OFFSET = OF_INSERT_PAGE_NO + 4;
    private static final int OF_INSERT_RAW = OF_INSERT_OFFSET + 2;

    public static byte[] insertLog(long xid, Page pg, byte[] raw) {
        byte[] logTypeRaw = {LOG_TYPE_INSERT};
        byte[] xidRaw = Parser.long2Byte(xid);
        byte[] pageNoRaw = Parser.int2Byte(pg.getPageNumber());
        byte[] offsetRaw = Parser.short2Byte(PageX.getFSO(pg));
        return Bytes.concat(logTypeRaw, xidRaw, pageNoRaw, offsetRaw, raw);
    }

    private static InsertLogInfo parseInsertLog(byte[] log) {
        InsertLogInfo li = new InsertLogInfo();
        // 8个字节
        li.xid = Parser.parseLong(Arrays.copyOfRange(log, OF_XID, OF_INSERT_PAGE_NO));
        // 4个字节
        li.pageNo = Parser.parseInt(Arrays.copyOfRange(log, OF_INSERT_PAGE_NO, OF_INSERT_OFFSET));
        // 2个字节
        li.offset = Parser.parseShort(Arrays.copyOfRange(log, OF_INSERT_OFFSET, OF_INSERT_RAW));
        // Data部分剩下的内容就是数据本身
        li.raw = Arrays.copyOfRange(log, OF_INSERT_RAW, log.length);
        return li;
    }

    private static void doInsertLog(PageCache pc, byte[] log, int flag) {
        InsertLogInfo li = parseInsertLog(log);
        Page pg = null;
        try {
            pg = pc.getPage(li.pageNo);
        } catch (Exception e) {
            Panic.panic(e);
        }
        try {
            // 大概意思就是如果是插入操作的UNDO，那么就是将这条记录标记位置1，表示被删除
            if (flag == UNDO) {
                DataItem.setDataItemRawInvalid(li.raw);
            }
            assert pg != null;
            PageX.recoverInsert(pg, li.raw, li.offset);
        } finally {
            assert pg != null;
            pg.release();
        }
    }
}
