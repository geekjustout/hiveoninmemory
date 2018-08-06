/**
 * 
 */
package com.innovativeintelli.hiveoninmemory.exception;

/**
 * @author Amithesh Merugu
 *
 */
public class InMemoryProcessFailedException extends Exception {
	/**
	 * Author : Amithesh Merugu
	 */
	private static final long serialVersionUID = 1L;
	
	public InMemoryProcessFailedException() {
		super();
	}

	public InMemoryProcessFailedException(String message, Throwable cause) {
		super(message, cause);
	}
	public InMemoryProcessFailedException(String message) {
		super(message);
	}
    public String getMessage() {
    	return super.getMessage();
    }
}
