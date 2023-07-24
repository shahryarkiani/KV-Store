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

    public String put(String key, String value) {
        var message = KVMessage.convertToMessage(key, value, KVMessage.MessageType.PUT);
        return sendRequest(message);
    }

    public String get(String key) {
        var message = KVMessage.convertToMessage(key, null, KVMessage.MessageType.GET);
        return sendRequest(message);
    }




    public String delete(String key) {
        var message = KVMessage.convertToMessage(key, null, KVMessage.MessageType.DELETE);
        return sendRequest(message);
    }

    private String sendRequest(ByteBuffer message) {
        message.flip();

        try {
            serverConn.write(message);
            readResponse(output, serverConn);

        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        output.flip();

        String responseValue = KVMessage.decodeResponse(output);

        output.compact();

        return responseValue;
    }

    public void close() {
        try {
            serverConn.close();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public static void readResponse(ByteBuffer output, SocketChannel serverConn) throws IOException {
        serverConn.read(output);

        int requiredLengthShort = output.getShort(0);

        if(requiredLengthShort != -1) {
            int requiredBytes = 0xFFFF & requiredLengthShort;
            int readBytes = output.position();

            while(readBytes < requiredBytes) {
                serverConn.read(output);
                readBytes = output.position();
            }
        }
    }

}
