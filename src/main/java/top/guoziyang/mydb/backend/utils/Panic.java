package top.guoziyang.mydb.backend.utils;

public class Panic {  // 相当于统一异常处理类
    public static void panic(Exception err) {
        err.printStackTrace();
        System.exit(1);
    }
}
