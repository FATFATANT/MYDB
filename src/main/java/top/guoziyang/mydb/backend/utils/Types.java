package top.guoziyang.mydb.backend.utils;

public class Types {
    public static long addressToUid(int pgno, short offset) {
        long u0 = pgno;
        long u1 = offset;
        return u0 << 32 | u1;
    }
}
