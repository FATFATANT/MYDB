package top.guoziyang.mydb.backend.tests;

public class DebugTest {
    public static void main(String[] args) {
        short a = (short) ((2) & ((1L << 16) - 1));

        System.out.println(a);
    }
}
