package com.huangjunyi1993.zeromq.util;

/**
 * 字节转换工具类（暂时没有用到）
 * Created by huangjunyi on 2022/8/21.
 */
public class ByteUtil {

    public static int bytesToInt(byte[] a){
        int ans=0;
        for(int i=0;i<4;i++){
            ans<<=8;
            ans|=(a[3-i]&0xff);
        }
        return ans;
    }




}
