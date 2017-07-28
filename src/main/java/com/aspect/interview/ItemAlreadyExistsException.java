package com.aspect.interview;

@SuppressWarnings("serial")
public class ItemAlreadyExistsException extends Exception {
	public ItemAlreadyExistsException(String message) {
		super(message);
	}
}
