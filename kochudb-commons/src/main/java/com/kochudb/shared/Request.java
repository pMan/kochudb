package com.kochudb.shared;

import java.io.Serializable;

public class Request implements Serializable {
    
    private static final long serialVersionUID = 1L;

    String command;
    String key;
    String value;

    public Request() {
    }

    public Request(String com, String key, String val) {
        this.command = com;
        this.key = key;
        this.value = val;
    }
    
    public byte[] getBytes() {
        if (command == null || key == null)
            throw new IllegalStateException("command and/or key can't be null");
        
        byte[] bytes = new byte[command.getBytes().length + Integer.SIZE + key.getBytes().length + value.getBytes().length];
        
        int pos = 0;
        System.arraycopy(command.getBytes(), 0, bytes, pos, command.getBytes().length);
        pos += command.getBytes().length;
        System.arraycopy(intToBytes(4, key.length()), 0, bytes, pos, Integer.BYTES);
        pos += Integer.BYTES;
        System.arraycopy(key.getBytes(), 0, bytes, pos, key.getBytes().length);
        pos += key.getBytes().length;
        System.arraycopy(value.getBytes(), 0, bytes, pos, value.getBytes().length);
        
        return bytes;
    }

    public static byte[] intToBytes(int resultLen, int in) {
        byte[] bytes = new byte[resultLen];
        for (int i = 0; i < resultLen; i++) {
            int cur = resultLen - i - 1;
            bytes[i] = (byte) ((in & 0xFF) >> (cur * 8));
        }
        return bytes;
    }
    
    public String getCommand() {
        return command;
    }

    public void setCommand(String command) {
        this.command = command;
    }

    public String getKey() {
        return key;
    }

    public void setKey(String key) {
        this.key = key;
    }

    public String getValue() {
        return value;
    }

    public void setValue(String value) {
        this.value = value;
    }

    @Override
    public String toString() {
        return "[key=" + key + ", value=" + value + ", command=" + command + "]";
    }
}
