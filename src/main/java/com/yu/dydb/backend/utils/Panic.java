package com.yu.dydb.backend.utils;

/**
 * 异常返回错误并退出程序
 */
public class Panic {
    public static void panic(Exception err) {
        err.printStackTrace();
        System.exit(1);
    }
}
