package org.apache.flink.runtime.consul;

import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;

import java.io.*;
import java.util.UUID;

final class ConsulLeaderData {

	private final String address;
	private final UUID sessionId;

	public ConsulLeaderData(String address, UUID sessionId) {
		this.address = address;
		this.sessionId = sessionId;
	}

	public static ConsulLeaderData from(byte[] bytes) {
		try {
			ByteArrayInputStream is = new ByteArrayInputStream(bytes);
			ObjectInputStream ois = new ObjectInputStream(is);
			String address = ois.readUTF();
			UUID sessionId = (UUID) ois.readObject();
			return new ConsulLeaderData(address, sessionId);
		} catch (IOException | ClassNotFoundException e) {
			throw new IllegalArgumentException("ConsulLeaderData deserialization failure", e);
		}
	}

	public byte[] toBytes() {
		ByteArrayOutputStream baos = new ByteArrayOutputStream();
		try {
			ObjectOutputStream oos = new ObjectOutputStream(baos);
			oos.writeUTF(address);
			oos.writeObject(sessionId);
			return baos.toByteArray();
		} catch (IOException e) {
			throw new IllegalStateException("ConsulLeaderData serialization failure", e);
		}
	}

	public String getAddress() {
		return address;
	}

	public UUID getSessionId() {
		return sessionId;
	}

	@Override
	public int hashCode() {
		return HashCodeBuilder.reflectionHashCode(this);
	}

	@Override
	public boolean equals(Object obj) {
		return EqualsBuilder.reflectionEquals(this, obj);
	}

	@Override
	public String toString() {
		return ToStringBuilder.reflectionToString(this, ToStringStyle.SIMPLE_STYLE);
	}
}
