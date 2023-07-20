package org.shahryarkiani;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class KVServerListener implements Runnable{

    private final ConcurrentLinkedQueue<SocketChannel> pendingConnections;

    private final ConcurrentSkipListMap<byte[], byte[]> kvStore;

    private final Selector selector;

    private final ExecutorService worker;

    public KVServerListener(ConcurrentSkipListMap<byte[], byte[]> store) {
        pendingConnections = new ConcurrentLinkedQueue<>();
        kvStore = store;
        worker = Executors.newSingleThreadExecutor();
        try {
            selector = Selector.open();
        } catch (IOException e) {
            System.err.println("[ERROR] Unable to initialize KVServerListener | " + e.getMessage());
            throw new RuntimeException(e);
        }
    }

    public void addConnection(SocketChannel newConn) {
        pendingConnections.add(newConn);
    }


    @Override
    public void run() {

        while(true) {

            SocketChannel newConnection = pendingConnections.poll();
            while(newConnection != null) {
                try {
                    newConnection.register(selector, SelectionKey.OP_READ, new ClientBuffer());
                } catch (ClosedChannelException err) {
                    System.err.println("[ERROR] " + err.getMessage());
                }
                System.out.println("[INFO] Client connection accepted");
                newConnection = pendingConnections.poll();
            }


            try {
                selector.select(100);
            } catch (IOException err) {
                throw new RuntimeException(err);
            }

            Set<SelectionKey> selectedKeys = selector.selectedKeys();

            for(var key : selectedKeys) {
                selectedKeys.remove(key);

                if(key.isReadable()) {

                    var channel = (SocketChannel)key.channel();

                    var clientBuf = (ClientBuffer)key.attachment();

                    ByteBuffer input = clientBuf.inputByteBuf;

                    int readBytes;

                    try{
                        readBytes = channel.read(input);
                        System.out.println("Read " + readBytes + " bytes");
                    } catch (IOException err) {
                        err.printStackTrace();
                        System.err.println("[ERROR] " + err.getMessage());
                        try {
                            channel.close();
                        } catch (IOException e) {
                            System.err.println("[ERROR] " + e.getMessage());
                        }
                        key.cancel();
                        continue;
                    }

                    if(readBytes == -1) {
                        System.out.println("[INFO] Client disconnected");
                        try {
                            channel.close();
                        } catch (IOException e) {
                            System.err.println("[ERROR] " + e.getMessage());
                        }
                        key.cancel();
                        continue;
                    }

                    if(clientBuf.messageReady()) {
                        var msgPair = clientBuf.readMessage();
                        System.out.println("Key: " + new String(msgPair[0]));
                        System.out.println("Value: " + new String(msgPair[1]));
                    }

                }
            }


        }

    }
}
