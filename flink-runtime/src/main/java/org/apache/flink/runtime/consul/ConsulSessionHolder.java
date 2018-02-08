package org.apache.flink.runtime.consul;

public final class ConsulSessionHolder {

	private volatile String sessionId;

	public String getSessionId() {
		return sessionId;
	}

	void setSessionId(String sessionId) {
		this.sessionId = sessionId;
	}
}
