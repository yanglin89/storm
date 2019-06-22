package com.run.stream.drpc;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ipc.RPC;

import java.io.IOException;

/**
 * rpc 服务
 */
public class RPCsever {

    public static void main(String[] args) throws IOException {

        Configuration configuration = new Configuration();

        RPC.Builder builder = new RPC.Builder(configuration);

        // java 的builder 模式
        RPC.Server server = builder.setProtocol(UserService.class)
                .setInstance(new UserServiceImpl())
                .setBindAddress("localhost")
                .setPort(9999)
                .build();

        /**
         * 启动 sever
         */
        server.start();

    }

}
