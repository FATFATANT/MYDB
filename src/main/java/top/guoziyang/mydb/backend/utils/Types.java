package top.guoziyang.mydb.backend.utils;

public class Types {
    public static long addressToUid(int pageNo, short offset) {
        // 这边就是将页号和页内偏移拼起来，这两个值中间有两个字节的值为全0
        return (long) pageNo << 32 | (long) offset;
    }
}
