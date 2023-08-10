package top.guoziyang.mydb.backend.common;

/*
       一个简单的共享数组，由于数组本身在Java中就是给出一个首地址
       数组就是一个对象，但是我们需要能够对其中的元素共享，因此只能
       把这个数组包进一个对象，通过指定索引下标来共享
*/
public class SubArray {
    public byte[] raw;
    public int start;
    public int end;

    public SubArray(byte[] raw, int start, int end) {
        this.raw = raw;
        this.start = start;
        this.end = end;
    }
}
