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
            output = ByteBuffer.allocate(1024);
        } catch (IOException e) {
            System.err.println("[ERROR] Unable to initialize client");
            throw new RuntimeException(e);
        }
    }

    public void sendMessage(String key, String value) {
        var message = KVMessage.convertToMessage(key, value);
        message.flip();

        try {
            serverConn.write(message);
            System.out.println("Message Sent");
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }


    public void close() {
        try {
            serverConn.close();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

}
