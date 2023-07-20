package org.shahryarkiani;

import java.nio.ByteBuffer;

public class ClientBuffer {

    protected final ByteBuffer inputByteBuf;

    protected final ByteBuffer outputByteBuf;

    public ClientBuffer() {
        inputByteBuf = ByteBuffer.allocateDirect(1024);
        outputByteBuf = ByteBuffer.allocateDirect(1024);
    }


    public boolean messageReady() {

        int bytesRead = inputByteBuf.position();

        //We don't have the message header
        if(bytesRead < 4)
            return false;

        int msgLen = 4 + inputByteBuf.getShort(0) + inputByteBuf.getShort(2);

        //We don't have the complete message yet, so we can't process it
        if(bytesRead < msgLen)
            return false;

        //The entire message is present and ready to process
        return true;

    }

    public byte[][] readMessage() {
        inputByteBuf.flip();

        var processedMessage = KVMessage.decodeMessage(inputByteBuf);

        inputByteBuf.compact();

        return processedMessage;
    }


}
