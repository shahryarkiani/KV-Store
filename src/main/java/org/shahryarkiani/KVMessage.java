package org.shahryarkiani;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

public class KVMessage {



    private static final int MAX_LENGTH = 65536 - 1;

    /*
    * Message format:
    * | 2 Bytes | 2 Bytes   | Key Len Bytes | Value Len Bytes |
    * | Key Len | Value Len |    key        |   Value         |
    *
    * This method simply converts a key value string pair into the message format detailed above
    * The encoding used is ascii, java default is utf-8, but utf-8 values up to 127 map to ascii
    */
    public static ByteBuffer convertToMessage(String key, String value) {
        if(key.length() > (MAX_LENGTH) || value.length() > MAX_LENGTH)
            throw new IllegalArgumentException("The message size is too long");

        ByteBuffer message = ByteBuffer.allocateDirect(4 + key.length()  + value.length());
        message.clear();

        message.putShort((short) key.length());
        message.putShort((short) value.length());
        message.put(key.getBytes(StandardCharsets.US_ASCII));
        message.put(value.getBytes(StandardCharsets.US_ASCII));


        return message;
    }

    public static byte[][] decodeMessage(ByteBuffer msg) {
        short keyLength = msg.getShort();
        short valueLength = msg.getShort();

        byte[] keyBytes = new byte[keyLength];
        byte[] valueBytes = new byte[valueLength];

        msg.get(keyBytes, 0, keyLength);
        msg.get(valueBytes, 0, valueLength);

        return new byte[][]{ keyBytes, valueBytes };
    }

}
