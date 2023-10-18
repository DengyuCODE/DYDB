package com.yu.dydb.backend.tm;

/**
 * 事务管理器
 * @author dy
 */
public interface TransactionManager {
    long begin(); //开启事务
    void commit(long xid); //提交事务
    void abort(long xid);  //取消一个事务
    boolean isActive(long xid); //查询事务状态是否为正在进行
    boolean isCommit(long xid); //查询事务状态是否已提交
    boolean isAbort(long xid); //查询事务状态是否已取消
    void close();              //关闭事务管理器
}
