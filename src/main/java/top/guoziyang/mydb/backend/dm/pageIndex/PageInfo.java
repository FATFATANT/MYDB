package top.guoziyang.mydb.backend.dm.pageIndex;

public class PageInfo {
    public int pageNo;
    public int freeSpace;

    public PageInfo(int pageNo, int freeSpace) {
        this.pageNo = pageNo;
        this.freeSpace = freeSpace;
    }
}
