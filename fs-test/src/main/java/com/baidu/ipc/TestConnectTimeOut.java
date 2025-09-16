package com.baidu.ipc;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.StandardSocketOptions;
import java.nio.channels.ServerSocketChannel;

public class TestConnectTimeOut {

    private static int port = 8989;
    private static int timeoutMS = 2 * 1000;
    // 20s
    private static void createServerSocket() throws IOException {
        InetSocketAddress address = new InetSocketAddress(port);
        // Create a new server socket and set to non blocking mode
        ServerSocketChannel acceptChannel = ServerSocketChannel.open();
        acceptChannel.configureBlocking(false);
        acceptChannel.setOption(StandardSocketOptions.SO_REUSEADDR, true);
        ServerSocket socket = acceptChannel.socket();
        socket.bind(address, 10);
    }

    public static void main(String[] args) throws IOException {
        createServerSocket();
        for(int i = 0; i < 20; i++) {
            clientToServer();
        }
    }

    private static void clientToServer() throws IOException {
        Socket socket = new Socket();
        long begin = System.currentTimeMillis();
        try {
            socket.connect(new InetSocketAddress("localhost", port), timeoutMS);
            // The following line can cause wait forever.
//            int  i = socket.getInputStream().read();
//            System.out.println("read " + i);
        } catch (Exception e) {
            e.printStackTrace();
            throw e;
        } finally {
            System.out.println("time used in ms: " + (System.currentTimeMillis() - begin));
        }
    }
}
