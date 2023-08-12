package top.guoziyang.mydb.backend.dm.page;

import java.util.Arrays;

import top.guoziyang.mydb.backend.dm.pageCache.PageCache;
import top.guoziyang.mydb.backend.utils.Parser;

/**
 * PageX管理普通页
 * 普通页结构
 * [FreeSpaceOffset] [Data]
 * FreeSpaceOffset: 2字节 空闲位置开始偏移
 */
public class PageX {

    private static final short OF_FREE = 0;
    private static final short OF_DATA = 2;
    public static final int MAX_FREE_SPACE = PageCache.PAGE_SIZE - OF_DATA;

    public static byte[] initRaw() {
        byte[] raw = new byte[PageCache.PAGE_SIZE];
        // Free Space Offset，空闲位置偏移
        setFSO(raw, OF_DATA);
        return raw;
    }

    private static void setFSO(byte[] raw, short ofData) {
        System.arraycopy(Parser.short2Byte(ofData), 0, raw, OF_FREE, OF_DATA);
    }

    // 获取pg的FSO
    public static short getFSO(Page pg) {
        return getFSO(pg.getData());
    }

    private static short getFSO(byte[] raw) {
        // 读取位于前两个字节的空闲位置偏移，其实就是前多少页已经写过数据了，后面要从哪里开始
        // 至于为什么是两个字节，我认为首先一个是一个页是8MB，这个不能用一个字节表示，需要13个bit，但是为了方便直接用两个字节表示
        return Parser.parseShort(Arrays.copyOfRange(raw, 0, 2));
    }

    // 将raw插入pg中，返回插入位置
    public static short insert(Page pg, byte[] raw) {
        pg.setDirty(true);
        short offset = getFSO(pg.getData());
        System.arraycopy(raw, 0, pg.getData(), offset, raw.length);
        setFSO(pg.getData(), (short) (offset + raw.length));
        return offset;
    }

    // 获取页面的空闲空间大小
    public static int getFreeSpace(Page pg) {
        return PageCache.PAGE_SIZE - (int) getFSO(pg.getData());
    }

    // 将raw插入pg中的offset位置，并将pg的offset设置为较大的offset
    public static void recoverInsert(Page pg, byte[] raw, short offset) {
        pg.setDirty(true);
        // 给pg的data追加raw字节数组
        System.arraycopy(raw, 0, pg.getData(), offset, raw.length);
        // 重新获取偏移量
        short rawFSO = getFSO(pg.getData());
        // 重新设置偏移量，也就是重新设置大小
        if (rawFSO < offset + raw.length) {
            setFSO(pg.getData(), (short) (offset + raw.length));
        }
    }

    // 将raw插入pg中的offset位置，不更新update
    public static void recoverUpdate(Page pg, byte[] raw, short offset) {
        pg.setDirty(true);
        // 这个没更新偏移量
        System.arraycopy(raw, 0, pg.getData(), offset, raw.length);
    }
}
