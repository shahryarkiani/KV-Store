package org.shahryarkiani;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.*;


public class KVServerListener implements Runnable{

    /*
    * The main server can't register new sockets to the selector directly since it might block,
    * so it submits new connections to this queue
     */
    private final ConcurrentLinkedQueue<SocketChannel> pendingConnections;

    private final ConcurrentSkipListMap<byte[], byte[]> kvStore;

    private final Selector selector;

    private final ExecutorService worker;

    public KVServerListener(ConcurrentSkipListMap<byte[], byte[]> store, ConcurrentLinkedQueue<SocketChannel> pendingConnections) {
        this.pendingConnections = pendingConnections;
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

        //These need to persist between while loops
        Queue<Future<Result>> completableTasks = new ArrayDeque<>();

        while(true) {

            SocketChannel newConnection = pendingConnections.poll();
            while(newConnection != null) {
                try {
                    newConnection.configureBlocking(false);
                    newConnection.register(selector, SelectionKey.OP_READ | SelectionKey.OP_WRITE, new ClientBuffer());
                } catch (IOException err) {
                    System.err.println("[ERROR] " + err.getMessage());
                }
                System.out.println("[INFO] Client connection accepted on " + Thread.currentThread().getName());
                newConnection = pendingConnections.poll();
            }


            try {
                //No timeout, if there's nothing ready for I/O we go through with the rest of the loop
                selector.select(1);
            } catch (IOException err) {
                throw new RuntimeException(err);
            }

            var iter = selector.selectedKeys().iterator();


            while(iter.hasNext()) {
                var key = iter.next();
                iter.remove();


                if(key.isWritable()) {
                    var channel = (SocketChannel) key.channel();

                    var clientBuf = (ClientBuffer) key.attachment();

                    ByteBuffer output = clientBuf.outputByteBuf;

                    output.flip();

                    try {
                        channel.write(output);
                    } catch (IOException err) {
                        err.printStackTrace();
                        System.err.println("[ERROR] " + err.getMessage());
                        key.cancel();
                        try {
                            channel.close();
                        } catch (IOException err2) {
                            err.printStackTrace();
                            System.err.println("[ERROR] " + err2.getMessage());
                        }
                        continue;
                    }

                    if(output.remaining() == 0) {
                        //We wrote everything out, we don't need to listen for readable
                        key.interestOps(SelectionKey.OP_READ);
                    }

                    output.compact();



                }

                if(key.isReadable()) {

                    var channel = (SocketChannel)key.channel();

                    var clientBuf = (ClientBuffer)key.attachment();

                    ByteBuffer input = clientBuf.inputByteBuf;

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

                    List<Task> tasks = new ArrayList<>(5);

                    //The socket might have multiple messages to read in it
                    while(clientBuf.messageReady()) {
                        var msgType = clientBuf.getMessageType();
                        var msgPair = clientBuf.readMessage();

                        tasks.add(new Task(msgType, msgPair[0], msgPair[1]));
                    }


                    //We want to do all the tasks as one unit of work to ensure that when the future is complete
                    //there won't be more writes to the clients buffer
                    if(!tasks.isEmpty()) {
                        var futureResult = worker.submit(() -> {
                            byte[][] results = new byte[tasks.size()][];
                            for(int i = 0; i < tasks.size(); i++) {
                                Task t = tasks.get(i);
                                switch (t.type()) {
                                    case GET -> {
                                        var result = kvStore.get(t.key());
                                        results[i] = result;
                                    }
                                    case PUT -> {
                                        kvStore.put(t.key(), t.value());
                                        results[i] = t.value();
                                    }
                                    case DELETE -> {
                                        var result = kvStore.remove(t.key());
                                        results[i] = result;
                                    }
                                }

                            }

                            return new Result(key, results);
                        });

                        completableTasks.add(futureResult);

                    } //tasks is empty

                }//Read operation
            }



            while(!completableTasks.isEmpty()) {

                var curTask = completableTasks.peek();

                //it's a single thread worker executor, so the tasks are guaranteed to execute sequentially
                //a queue is FIFO, so if a task isn't done, the stuff submitted after won't be either
                if(!curTask.isDone())
                    break;

                //the task we peeked is done, so we remove it
                completableTasks.poll();

                Result result;

                try {
                    result = curTask.get();
                } catch (InterruptedException | ExecutionException e) {
                    e.printStackTrace();
                    System.out.println("[ERROR] " + e.getMessage());
                    continue;
                }

                var key = result.key();
                byte[][] responseBytesList = result.result();

                key.interestOps(SelectionKey.OP_READ | SelectionKey.OP_WRITE);

                ByteBuffer outputBuffer = ((ClientBuffer)key.attachment()).outputByteBuf;

                for(var responseByte : responseBytesList) {
                    if(responseByte == null) {
                        outputBuffer.putShort((short) -1);
                        continue;
                    }
                    outputBuffer.putShort((short)(responseByte.length & 0xFFFF));
                    outputBuffer.put(responseByte);
                }

            }//handle completed tasks loop



        }//While loop

    }
}


record Task(KVMessage.MessageType type, byte[] key, byte[] value) {}

record Result(SelectionKey key, byte[][] result) {}