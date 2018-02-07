package org.apache.flink.runtime.consul;

import java.util.UUID;

public interface ConsulLeaderLatchListener {

	void onLeadershipAcquired(String address, UUID sessionId);

	void onLeadershipRevoked();

	void onLeaderResolved(String address, UUID sessionId);

	void onError(Exception exception);
}
