package com.yu.dydb.backend.tm;

import com.yu.dydb.backend.utils.Panic;
import com.yu.dydb.backend.utils.Parser;
import com.yu.dydb.common.Error;

import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class TransactionManagerImpl implements TransactionManager{
    //定义一些常量
    //XID文件头长度
    static final int LEN_XID_HEADER_LENGTH = 8;
    //每个事务的占用长度
    private static final int XID_FIELD_SIZE = 1;
    //事务的三种状态
    private static final byte FIELD_TRAN_ACTIVE = 0;
    private static final byte FIELD_TRAN_COMMITTED = 1;
    private static final byte FIELD_TRAN_ABORT = 2;
    //超级事务,状态永远为committed
    private static final long SUPER_XID = 0;
    //XID文档后缀
    static final String XID_SUFFIX = ".xid";

    private RandomAccessFile file;
    private FileChannel fc; //nio包下
    private long xidCounter; //xid计数器
    private Lock counterLock;


    public TransactionManagerImpl(RandomAccessFile file, FileChannel fc){
        this.file = file;
        this.fc = fc;
        counterLock = new ReentrantLock();
        checkXIDCounter();
    }

    /**
     * 对XID文件进行校验，保证文件合法性
     * 校验方法:通过文件头的8字节数字反推文件的理论长度，与文件的实际长度做对比，不同则不合法
     */
    private void checkXIDCounter(){
        long fileLen = 0;
        try{
            fileLen = file.length();
        }catch (IOException e){
            Panic.panic(Error.BadXIDFileException);
        }
        if(fileLen < LEN_XID_HEADER_LENGTH){
            Panic.panic(Error.BadXIDFileException);
        }

        ByteBuffer buf = ByteBuffer.allocate(LEN_XID_HEADER_LENGTH);
        try{
            fc.position(0);//指定读取文件的起始位置
            fc.read(buf);
        }catch (IOException e){
            Panic.panic(e);
        }
        this.xidCounter = Parser.parseLong(buf.array());
    }

    @Override
    public long begin() {
        return 0;
    }

    @Override
    public void commit(long xid) {

    }

    @Override
    public void abort(long xid) {

    }

    @Override
    public boolean isActive(long xid) {
        return false;
    }

    @Override
    public boolean isCommit(long xid) {
        return false;
    }

    @Override
    public boolean isAbort(long xid) {
        return false;
    }

    @Override
    public void close() {

    }
}
