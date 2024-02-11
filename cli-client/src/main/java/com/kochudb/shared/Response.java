package com.kochudb.shared;

import java.io.Serializable;

public class Response implements Serializable {
	private static final long serialVersionUID = 1L;

	String command;
	String key;
	String value;
	String data;
	
	public Response() {}
	
	public Response(String com, String key, String val, String data) {
		this.command = com;
		this.key = key;
		this.value = val;
		this.data = data;
	}
	
	public Response(Request request) {
		this.command = request.command;
		this.key = request.key;
		this.value = request.value;
	}
	
	public String getData() {
		return this.data;
	}
	
	public void setData(String data) {
		this.data = data;
	}
	
	@Override
	public String toString() {
		return "[key=" + key + ", value=" + value + ", command=" + command
				+ ", data=" + data + "]";
	}
	
}
