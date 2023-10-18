package com.yu.dydb.backend.tm;

import com.yu.dydb.backend.utils.Panic;
import com.yu.dydb.backend.utils.Parser;
import com.yu.dydb.common.Error;
import lombok.Data;

import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
@Data
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
     * 对于校验未通过的，会通过panic方法退出程序(System.exit(1))
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
        //获取xid计数
        this.xidCounter = Parser.parseLong(buf.array());
        long end = getXidPosition(this.xidCounter + 1);
        if(end != fileLen){
            Panic.panic(Error.BadXIDFileException);
        }
    }

    //根据事务xid获取其在xid文件中对应的位置
    public long getXidPosition(long xid){
        return LEN_XID_HEADER_LENGTH + (xid - 1)*XID_FIELD_SIZE;
    }

    /**
     * 开始一个事务
     * 设置xidCounter+1事务的状态为active，然后xidCounter自增，更新文件
     * @return
     */
    @Override
    public long begin() {
        //开始一个事务
        //将该过程锁住(获取事务xid)
        counterLock.lock();
        try{
            long xid = xidCounter + 1;//得到当前事务xid
            updateXID(xid, FIELD_TRAN_ACTIVE);//更新状态为活跃
            incrXIDCounter();//更新文件头
            return xid;
        }finally {
            counterLock.unlock();
        }
    }

    //更新事务状态
    private void updateXID(long xid, byte status) {
        long offset = getXidPosition(xid);
        byte[] tmp = new byte[XID_FIELD_SIZE];
        tmp[0] = status;
        ByteBuffer buf = ByteBuffer.wrap(tmp);
        try{
            fc.position(offset);
            fc.write(buf);
        }catch (IOException e){
            Panic.panic(e);
        }
        try{
            //这里的所有文件操作，在执行后都需要立刻刷入文件中，防止在崩溃后文件丢失数据，
            // fileChannel 的 force() 方法，强制同步缓存内容到文件中，
            // 类似于 BIO 中的 flush() 方法
            fc.force(false);
        }catch (IOException e){
            Panic.panic(e);
        }
    }

    //将XID加一，并更新XID header
    private void incrXIDCounter() {
        xidCounter++;
        ByteBuffer buf = ByteBuffer.wrap(Parser.long2Byte(xidCounter));
        try{
            fc.position(0);
            fc.write(buf);
        }catch (IOException e){
            Panic.panic(e);
        }
        try{
            fc.force(false);//刷新到文件中
        }catch (IOException e){
            Panic.panic(e);
        }
    }

    //检查XID事务是否处于status状态
    private boolean checkXID(long xid, byte status){
        //获取xid的偏移地址
        long offset = getXidPosition(xid);
        ByteBuffer buf = ByteBuffer.wrap(new byte[XID_FIELD_SIZE]);
        try{
            fc.position(offset);
            fc.write(buf);
        }catch (IOException e){
            Panic.panic(e);
        }
        return buf.array()[0] == status;
    }

    /**
     * 提交事务
     * @param xid
     */
    @Override
    public void commit(long xid) {
        updateXID(xid,FIELD_TRAN_COMMITTED);
    }

    @Override
    public void abort(long xid) {
        updateXID(xid,FIELD_TRAN_ABORT);
    }

    @Override
    public boolean isActive(long xid) {
        return checkXID(xid,FIELD_TRAN_ACTIVE);
    }

    @Override
    public boolean isCommit(long xid) {
        return checkXID(xid,FIELD_TRAN_COMMITTED);
    }

    @Override
    public boolean isAbort(long xid) {
        return checkXID(xid,FIELD_TRAN_ABORT);
    }

    @Override
    public void close() {
        try{
            fc.close();
            file.close();
        }catch (IOException e){
            Panic.panic(e);
        }
    }
}
