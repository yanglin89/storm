package com.run.stream.drpc;

import org.apache.storm.Config;
import org.apache.storm.generated.AuthorizationException;
import org.apache.storm.generated.DRPCExecutionException;
import org.apache.storm.thrift.TException;
import org.apache.storm.thrift.transport.TTransportException;
import org.apache.storm.utils.DRPCClient;

/**
 * 远程 drpc 客户端调用程序
 */
public class RemoteDRPCclient {

    public static void main(String[] args) {

        Config config = new Config();

        /**
         * 需要添加下面一组配置，否则会报空指针错误
         */
        config.put("storm.thrift.transport","org.apache.storm.security.auth.SimpleTransportPlugin");
        config.put(Config.STORM_NIMBUS_RETRY_TIMES,3); // NUMBUS 重试3次
        config.put(Config.STORM_NIMBUS_RETRY_INTERVAL,10);
        config.put(Config.STORM_NIMBUS_RETRY_INTERVAL_CEILING,20);
        config.put(Config.DRPC_MAX_BUFFER_SIZE,1048576);


        try {
            DRPCClient client = new DRPCClient(config,"master",3772);
            String result = client.execute("userAdd","lisi");

            System.out.println("client invoked : " + result);

        } catch (TTransportException e) {
            e.printStackTrace();
        } catch (DRPCExecutionException e) {
            e.printStackTrace();
        } catch (AuthorizationException e) {
            e.printStackTrace();
        } catch (TException e) {
            e.printStackTrace();
        }

    }

}
