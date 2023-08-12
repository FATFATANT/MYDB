package top.guoziyang.mydb.backend.tm;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import top.guoziyang.mydb.backend.utils.Panic;
import top.guoziyang.mydb.backend.utils.Parser;
import top.guoziyang.mydb.common.Error;

public class TransactionManagerImpl implements TransactionManager {

    // XID文件头长度
    static final int LEN_XID_HEADER_LENGTH = 8;
    // 每个事务的占用长度
    private static final int XID_FIELD_SIZE = 1;

    // 事务的三种状态
    private static final byte FIELD_TRAN_ACTIVE = 0;
    private static final byte FIELD_TRAN_COMMITTED = 1;
    private static final byte FIELD_TRAN_ABORTED = 2;

    // 超级事务，永远为committed状态
    public static final long SUPER_XID = 0;
    static final String XID_SUFFIX = ".xid";
    private final RandomAccessFile file;
    private final FileChannel fc;
    private long xidCounter;
    private final Lock counterLock;

    TransactionManagerImpl(RandomAccessFile raf, FileChannel fc) {
        this.file = raf;
        this.fc = fc;
        counterLock = new ReentrantLock();
        checkXIDCounter();  // 为啥要传入一个空的文件
    }

    /**
     * 检查XID文件是否合法
     * 读取XID_FILE_HEADER中的xidcounter，根据它计算文件的理论长度，对比实际长度
     */
    private void checkXIDCounter() {
        long fileLen = 0;
        try {
            fileLen = file.length();
        } catch (IOException e1) {
            Panic.panic(Error.BadXIDFileException);  // 看个文件长度也会爆出异常？
        }
        if (fileLen < LEN_XID_HEADER_LENGTH) {
            Panic.panic(Error.BadXIDFileException);
        }
        /*
            下面几句大概意思就是说将xid文件的前8个字节读入内存
            这8个字节表示db中的事务数，因此可以parseLong把它读出来
         */
        ByteBuffer buf = ByteBuffer.allocate(LEN_XID_HEADER_LENGTH);
        try {
            fc.position(0);
            fc.read(buf);  // 注意是从channel读到buffer
        } catch (IOException e) {
            Panic.panic(e);
        }
        // 每次进入这个方法相当于读出来当前的事务数，后续开启新的事务就在这个事务编号的基础上+1
        this.xidCounter = Parser.parseLong(buf.array());
        // 最后一个事务所在字节下标，应与文件大小一致
        long end = getXidPosition(this.xidCounter + 1);
        if (end != fileLen) {
            Panic.panic(Error.BadXIDFileException);
        }
    }

    /*
        根据事务xid取得其在xid文件中对应的位置
        正如文档所写，前8个字节记录了这个 XID 文件管理的事务的个数
        后面的每个字节表示每个事务的状态（每个事务的状态用一个字节就能表示）
        又由于事务0是保留事务，这里就需要-1
     */
    private long getXidPosition(long xid) {
        // 每个事务的事务id占一个字节
        return LEN_XID_HEADER_LENGTH + (xid - 1) * XID_FIELD_SIZE;
    }

    /*
        更新xid事务的状态为status
        可以发现它这里的事务无论是启动提交还是关闭都是调用这个update
     */
    private void updateXID(long xid, byte status) {
        long offset = getXidPosition(xid);
        byte[] tmp = new byte[XID_FIELD_SIZE];
        // 由于就是一个字节来表示事务状态，而事务的大小也是一个字节，这里就刚好而且写死了
        tmp[0] = status;
        // 将这个事务状态对应的字节数组包进Buffer然后写入xid这个文件的fileChannel，然后通过force落盘
        ByteBuffer buf = ByteBuffer.wrap(tmp);
        try {
            fc.position(offset);
            fc.write(buf);
        } catch (IOException e) {
            Panic.panic(e);
        }
        try {
            fc.force(false);  // metaData表示是否同步文件的元数据（例如最后修改时间等）
        } catch (IOException e) {
            Panic.panic(e);
        }
    }

    // 将XID加一，并更新XID Header
    private void incrXIDCounter() {
        xidCounter++;
        ByteBuffer buf = ByteBuffer.wrap(Parser.long2Byte(xidCounter));
        try {
            fc.position(0);  // 更新事务数量就是直接替换原来的8个字节
            fc.write(buf);
        } catch (IOException e) {
            Panic.panic(e);
        }
        try {
            fc.force(false);
        } catch (IOException e) {
            Panic.panic(e);
        }
    }

    // 开始一个事务，并返回XID
    public long begin() {
        counterLock.lock();
        try {
            long xid = xidCounter + 1;
            updateXID(xid, FIELD_TRAN_ACTIVE);
            incrXIDCounter();
            return xid;
        } finally {
            counterLock.unlock();
        }
    }

    // 提交XID事务
    public void commit(long xid) {
        updateXID(xid, FIELD_TRAN_COMMITTED);
    }

    // 回滚XID事务
    public void abort(long xid) {
        updateXID(xid, FIELD_TRAN_ABORTED);
    }

    // 检测XID事务是否处于status状态
    private boolean checkXID(long xid, byte status) {
        // 查找事务id在xid文件对应转换成的字节数组的索引，然后取出该索引上的字节，就获得了该事务的状态
        long offset = getXidPosition(xid);
        ByteBuffer buf = ByteBuffer.wrap(new byte[XID_FIELD_SIZE]);
        try {
            fc.position(offset);
            fc.read(buf);
        } catch (IOException e) {
            Panic.panic(e);
        }
        // 检查事务在磁盘中存储的状态和传入的状态是否相等
        return buf.array()[0] == status;
    }

    public boolean isActive(long xid) {
        // 无事务就是永远活跃
        if (xid == SUPER_XID) return false;
        return checkXID(xid, FIELD_TRAN_ACTIVE);
    }

    public boolean isCommitted(long xid) {
        // 无事务也是永远已提交
        if (xid == SUPER_XID) return true;
        return checkXID(xid, FIELD_TRAN_COMMITTED);
    }

    public boolean isAborted(long xid) {
        // 无事务不会已撤销
        if (xid == SUPER_XID) return false;
        return checkXID(xid, FIELD_TRAN_ABORTED);
    }

    public void close() {
        try {
            fc.close();
            file.close();
        } catch (IOException e) {
            Panic.panic(e);
        }
    }

}
