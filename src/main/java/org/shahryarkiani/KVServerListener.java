package org.shahryarkiani;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;
import java.nio.charset.StandardCharsets;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ConcurrentSkipListMap;

public class KVServerListener implements Runnable{

    private final ConcurrentLinkedQueue<SocketChannel> pendingConnections;

    private final ConcurrentSkipListMap<byte[], byte[]> kvStore;

    private final Selector selector;

    public KVServerListener(ConcurrentSkipListMap<byte[], byte[]> store) {
        pendingConnections = new ConcurrentLinkedQueue<>();
        kvStore = store;
        try {
            selector = Selector.open();
        } catch (IOException e) {
            System.err.println("[ERROR] Unable to initialize KVServerListener");
            throw new RuntimeException(e);
        }
    }

    public void addConnection(SocketChannel newConn) {
        pendingConnections.add(newConn);
    }


    @Override
    public void run() {



        while(true) {

            SocketChannel newConn = pendingConnections.poll();
            while(newConn != null) {
                try {
                    newConn.register(selector, SelectionKey.OP_READ);
                } catch (ClosedChannelException err) {
                    System.err.println("[ERROR] " + err.getMessage());
                }
                System.out.println("[INFO] Client connection accepted");
                newConn = pendingConnections.poll();
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
                    ByteBuffer input = ByteBuffer.allocate(256);

                    int readBytes;

                    try{
                        readBytes = channel.read(input);
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

                    input.flip();

                    System.out.println(StandardCharsets.UTF_8.decode(input));

                }
            }


        }

    }
}
