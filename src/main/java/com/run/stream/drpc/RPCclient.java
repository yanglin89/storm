package com.run.stream.drpc;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ipc.RPC;

import java.io.IOException;
import java.net.InetSocketAddress;

/**
 * rpc 客户端服务
 */
public class RPCclient {

    public static void main(String[] args) throws IOException {

        Configuration configuration = new Configuration();

        long clientVersion = 88888888;

        /**
         * 获取到远程服务
         */
        UserService userService = RPC.getProxy(UserService.class,clientVersion,
                new InetSocketAddress("localhost",9999),
                configuration);

        userService.userAdd("zhangsan","30");
        System.out.println("client success ");

        /**
         * 关闭客户端 rpc 连接
         */
        RPC.stopProxy(UserService.class);
    }

}
