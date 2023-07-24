package org.shahryarkiani;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ConcurrentSkipListMap;

public class KVServer implements Runnable {


    private final ConcurrentSkipListMap<byte[], byte[]> kvStore;

    private final int port;

    private static final long timeout = 10 * 1000;

    private final ServerSocketChannel listener;

    private final Selector selector;

    private final KVServerListener[] handlers;

    private int curHandler;

    public KVServer(int port, int eventGroupSize) {
        kvStore = new ConcurrentSkipListMap<>((a, b) -> {
            int len = Math.min(a.length, b.length);

            for(int i = 0; i < len; i++) {
                int cmp = Byte.compare(a[i], b[i]);
                if(cmp != 0)
                    return cmp;
            }


            return Integer.compare(a.length, b.length);
        });

        handlers = new KVServerListener[eventGroupSize];

        ConcurrentLinkedQueue<SocketChannel> connectionsQueue = new ConcurrentLinkedQueue<>();

        for(int i = 0; i < handlers.length; i++) {
            handlers[i] = new KVServerListener(kvStore, connectionsQueue);
        }

        curHandler = 0;

        this.port = port;
        try {
            listener = ServerSocketChannel.open();
            selector = Selector.open();
        } catch (IOException e) {
            System.err.println("[ERROR] Unable to open listener socket or selector");
            throw new RuntimeException(e);
        }
    }

    private void configureListenerAndSelector() {


        try {
            listener.configureBlocking(false);
            listener.socket().bind(new InetSocketAddress("127.0.0.1", port));
            listener.register(selector, SelectionKey.OP_ACCEPT);
        } catch (IOException err) {
            err.printStackTrace();
            System.err.println("[ERROR] " + err.getMessage());
        }
    }
    public void run() {




        configureListenerAndSelector();

        try {
            System.out.println("[INFO] Server config complete, starting server on " + listener.getLocalAddress().toString());
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        for(KVServerListener serverListener : handlers) {
            Thread handlerThread = new Thread(serverListener);
            handlerThread.start();
        }



        while(true) {

            try {
                selector.select(timeout);
            } catch (IOException err) {
                throw new RuntimeException(err);
            }

            Set<SelectionKey> selectedKeys = selector.selectedKeys();

            if(selectedKeys.isEmpty()) {
                System.out.println("[INFO] No connection attempts for last " + (timeout/1000) + " seconds");
                continue;
            }

            for(var key : selectedKeys) {

                selectedKeys.remove(key);

                if(key.isAcceptable()) {
                    try {
                        SocketChannel newConnection = listener.accept();


                        curHandler %= handlers.length;

                        System.out.println("Sent connection to Thread-" + curHandler);

                        handlers[curHandler++].addConnection(newConnection);

                    } catch (IOException err) {
                        err.printStackTrace();
                        System.err.println("[ERROR] " + err.getMessage());
                    }
                }
            }

        }

    }


}
