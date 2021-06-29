package com.easytocourse.model;

import java.util.List;

public class EmployeeErrorResponse {
	
	private long timestamp;
	private String message;
	private List<String> errors;
	public long getTimestamp() {
		return timestamp;
	}
	public void setTimestamp(long timestamp) {
		this.timestamp = timestamp;
	}
	public String getMessage() {
		return message;
	}
	public void setMessage(String message) {
		this.message = message;
	}
	public List<String> getErrors() {
		return errors;
	}
	public void setErrors(List<String> errors) {
		this.errors = errors;
	}
	public EmployeeErrorResponse(long timestamp, String message, List<String> errors) {
		super();
		this.timestamp = timestamp;
		this.message = message;
		this.errors = errors;
	}
	public EmployeeErrorResponse() {
		super();
		// TODO Auto-generated constructor stub
	}
	

}
