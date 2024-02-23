package com.kochudb.shared;

import java.io.Serial;
import java.io.Serializable;

public class Request implements Serializable {

    @Serial
    private static final long serialVersionUID = 1L;
    
    String command;
    String key;
    String value;
    
    public Request() {}
    
    public Request(String com, String key, String val) {
        this.command = com;
        this.key = key;
        this.value = val;
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
        return "//" + command + " " + key + " " + (value == null ? "" : value);
    }
}
