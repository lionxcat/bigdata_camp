package com.lionxcat;

import java.net.InetSocketAddress;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ipc.RPC;

public class RpcDemo {
    public static void main(String[] args) throws Exception {
        RPC.Server server = getServer();

        new Thread(() -> {
            System.out.println("[server] starting...");
            server.start();
        }).start();

        Thread.sleep(1000);
        new Thread(() -> {
            System.out.println("[client] starting...");
            startClient();
            server.stop();
            System.out.println("[server] stopped.");
        }).start();

        System.in.read();
    }

    private static RPC.Server getServer() throws Exception {
        RPC.Builder builder = new RPC.Builder(new Configuration());
        builder.setBindAddress("127.0.0.1");
        builder.setPort(12345);
        builder.setProtocol(GetNameRpcIF.class);
        builder.setInstance(new GetNameRpc());
        return builder.build();
    }

    private static void startClient() {
        try {
            GetNameRpcIF proxy = RPC.getProxy(GetNameRpcIF.class, GetNameRpcIF.versionID,
                    new InetSocketAddress("127.0.0.1", 12345), new Configuration());
            String name;
            name = proxy.findName(20210123456789L);
            System.out.println("[client] id: 20210123456789, name:" + name);
            name = proxy.findName(20210607020479L);
            System.out.println("[client] id: 20210607020479, name:" + name);
            name = proxy.findName(20210000000000L);
            System.out.println("[client] id: 20210000000000, name:" + name);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
