package com.weblog;

public class WashException extends RuntimeException {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	public WashException(String message) {
        super(message);
    }
	
	public WashException(String message, Throwable cause) {
        super(message, cause);
    }
	
    public WashException(Throwable cause) {
        super(cause);
    }
}
