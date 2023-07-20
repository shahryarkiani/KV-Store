package org.shahryarkiani;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;

public class KVClient {


    private final SocketChannel serverConn;

    private final ByteBuffer output;

    public KVClient(String address, int port) {
        try {
            serverConn = SocketChannel.open(new InetSocketAddress(address, port));
            output = ByteBuffer.allocate(256);
        } catch (IOException e) {
            System.err.println("[ERROR] Unable to initialize client");
            throw new RuntimeException(e);
        }
    }

    public void sendMessage(String key, String value) {

        var charBuf = output.asCharBuffer();

        charBuf.append(key);
        charBuf.append("::");
        charBuf.append(value);

        try {
            serverConn.write(output);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

    }

}
