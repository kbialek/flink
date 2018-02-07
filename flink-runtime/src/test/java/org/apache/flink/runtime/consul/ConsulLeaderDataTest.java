package org.apache.flink.runtime.consul;

import org.junit.Test;

import java.util.UUID;

import static org.junit.Assert.assertEquals;

public class ConsulLeaderDataTest {

	@Test
	public void testSerialization() {
		String address = "address-1";
		UUID sessionId = UUID.randomUUID();

		ConsulLeaderData data = new ConsulLeaderData(address, sessionId);

		ConsulLeaderData deser = ConsulLeaderData.from(data.toBytes());

		assertEquals(address, deser.getAddress());
		assertEquals(sessionId, deser.getSessionId());
	}

}
