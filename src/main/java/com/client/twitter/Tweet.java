package com.client.twitter;

public class Tweet {
	private String statusText;
	private String statusImage;
	
	public Tweet(String statusText, String statusImage) {
		super();
		this.statusText = statusText;
		this.statusImage = statusImage;
	}
	public String getStatusText() {
		return statusText;
	}
	public void setStatusText(String statusText) {
		this.statusText = statusText;
	}
	public String getStatusImage() {
		return statusImage;
	}
	public void setStatusImage(String statusImage) {
		this.statusImage = statusImage;
	}
	@Override
	public String toString() {
		return "Tweet [statusText=" + statusText + ", statusImage="
				+ statusImage + "]";
	}
	
}
