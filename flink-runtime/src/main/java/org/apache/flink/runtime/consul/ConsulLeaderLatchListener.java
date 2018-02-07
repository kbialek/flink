package org.apache.flink.runtime.consul;

import java.util.UUID;

public interface ConsulLeaderLatchListener {

	void onLeadershipAcquired();

	void onLeadershipRevoked();

	void onLeaderResolved(String address, UUID sessionId);
}
