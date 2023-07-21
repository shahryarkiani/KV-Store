package org.shahryarkiani;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

public class KVMessage {



    public enum MessageType {
        PUT, GET, DELETE
    }

    private static final int MAX_LENGTH = 65536 - 1;

    /*
    * Message format:
    * | 2 Bytes | 2 Bytes   | Key Len Bytes | Value Len Bytes |
    * | Key Len | Value Len |    key        |   Value         |
    *
    * This method simply converts a key value string pair into the message format detailed above
    * The encoding used is ascii, java default is utf-8, but utf-8 values up to 127 map to ascii anyways
    *
    * If the value length bits are all 0, the message is a GET for the key
    * If the value length bits are all 1, the message is a DELETE for the key
    * The default is a PUT for the Key and Value provided
    *
    */
    public static ByteBuffer convertToMessage(String key, String value, MessageType msgType) {

        int keyLength = key.length();
        int valueLength = switch (msgType) {
            case GET -> 0;
            case DELETE -> -1;
            case PUT -> value.length();
        };

        var message = ByteBuffer.allocateDirect(4 + keyLength + (valueLength == -1 ? 0 : valueLength));

        message.putShort((short) (keyLength & 0xFFFF));
        message.putShort((short) (valueLength & 0xFFFF));
        message.put(key.getBytes(StandardCharsets.US_ASCII));
        if(msgType == MessageType.PUT)
            message.put(value.getBytes(StandardCharsets.US_ASCII));

        return message;
    }
    public static byte[][] decodeMessage(ByteBuffer msg) {
        int keyLength = 0xFFFF & msg.getShort();
        int valueLength = 0xFFFF & msg.getShort();

        if(valueLength == 0 || valueLength == MAX_LENGTH) {
            byte[] keyBytes = new byte[keyLength];

            msg.get(keyBytes, 0, keyLength);

            return new byte[][]{ keyBytes, null};
        }

        byte[] keyBytes = new byte[keyLength];
        byte[] valueBytes = new byte[valueLength];

        msg.get(keyBytes, 0, keyLength);
        msg.get(valueBytes, 0, valueLength);

        return new byte[][]{ keyBytes, valueBytes };
    }

}
