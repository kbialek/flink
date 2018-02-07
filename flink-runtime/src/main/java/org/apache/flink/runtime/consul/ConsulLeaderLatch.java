package org.apache.flink.runtime.consul;

import com.ecwid.consul.v1.ConsulClient;
import com.ecwid.consul.v1.OperationException;
import com.ecwid.consul.v1.QueryParams;
import com.ecwid.consul.v1.Response;
import com.ecwid.consul.v1.kv.model.GetBinaryValue;
import com.ecwid.consul.v1.kv.model.PutParams;
import com.ecwid.consul.v1.session.model.NewSession;
import com.google.common.base.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.UUID;
import java.util.concurrent.Executor;

public final class ConsulLeaderLatch {

	private static final Logger LOG = LoggerFactory.getLogger(ConsulLeaderLatch.class);


	private final ConsulClient client;

	private final Executor executor;

	private final String leaderKey;

	private final String nodeAddress;

	/**
	 * SessionID used for communication with Consul
	 */
	private String consulSessionId;

	/**
	 * SessionID
	 */
	private UUID flinkSessionId;

	private long leaderKeyIndex;

	private volatile boolean runnable;

	private boolean hasLeadership;

	private ConsulLeaderData leaderData;

	private final ConsulLeaderLatchListener listener;

	private final int waitTime;

	/**
	 * @param client      Consul client
	 * @param executor    Executor to run background tasks
	 * @param leaderKey   key in Consul KV store
	 * @param nodeAddress leadership changes are reported to this contender
	 * @param waitTime    Consul blocking read timeout (in seconds)
	 */
	public ConsulLeaderLatch(ConsulClient client,
							 Executor executor,
							 String leaderKey,
							 String nodeAddress,
							 ConsulLeaderLatchListener listener,
							 int waitTime) {
		this.client = Preconditions.checkNotNull(client, "client");
		this.executor = Preconditions.checkNotNull(executor, "executor");
		this.leaderKey = Preconditions.checkNotNull(leaderKey, "leaderKey");
		this.nodeAddress = Preconditions.checkNotNull(nodeAddress, "nodeAddress");
		this.listener = Preconditions.checkNotNull(listener, "listener");
		this.waitTime = waitTime;
	}

	public void start() {
		LOG.info("Starting Consul Leadership Latch");
		runnable = true;
		executor.execute(this::watch);
	}

	public void stop() {
		LOG.info("Stopping Consul Leadership Latch");
		runnable = false;
		client.sessionDestroy(consulSessionId, QueryParams.DEFAULT);
	}

	private void watch() {
		while (runnable) {
			createConsulSessionIfNecessary();
			GetBinaryValue value = readLeaderKey();
			String leaderSessionId = null;
			if (value != null) {
				leaderKeyIndex = value.getModifyIndex();
				leaderSessionId = value.getSession();
			}

			if (runnable) {
				if (leaderSessionId == null) {
					LOG.info("No hasLeadership elected. Current node is trying to register");
					Boolean success = writeLeaderKey();
					if (success) {
						leadershipAcquired();
					} else {
						leadershipRevoked();
					}
				} else if (!hasLeadership) {
					leaderResolved(ConsulLeaderData.from(value.getValue()));
				}
			}
		}
	}

	public boolean hasLeadership() {
		return hasLeadership;
	}

	private void createConsulSessionIfNecessary() {
		if (consulSessionId == null) {
			NewSession newSession = new NewSession();
			newSession.setName("flink");
			consulSessionId = client.sessionCreate(newSession, QueryParams.DEFAULT).getValue();
			flinkSessionId = UUID.randomUUID();
		}
	}

	private GetBinaryValue readLeaderKey() {
		QueryParams queryParams = QueryParams.Builder.builder()
			.setIndex(leaderKeyIndex)
			.setWaitTime(waitTime)
			.build();
		Response<GetBinaryValue> leaderKeyValue = client.getKVBinaryValue(leaderKey, queryParams);
		return leaderKeyValue.getValue();
	}

	private Boolean writeLeaderKey() {
		PutParams putParams = new PutParams();
		putParams.setAcquireSession(consulSessionId);
		try {
			ConsulLeaderData data = new ConsulLeaderData(nodeAddress, flinkSessionId);
			return client.setKVBinaryValue(leaderKey, data.toBytes(), putParams).getValue();
		} catch (OperationException ex) {
			return false;
		}
	}

	private void leadershipAcquired() {
		if (!hasLeadership) {
			hasLeadership = true;
			notifyOnLeadershipAcquired();
			LOG.info("Cluster leadership has been acquired by current node");
		}
	}

	private void leadershipRevoked() {
		if (hasLeadership) {
			hasLeadership = false;
			notifyOnLeadershipRevoked();
			LOG.info("Cluster leadership has been revoked from current node");
		}
	}

	private void leaderResolved(ConsulLeaderData data) {
		if (!data.equals(leaderData)) {
			leaderData = data;
			notifyOnLeaderResolved(data);
			LOG.info("Cluster hasLeadership resolved {}", data);
		}
	}

	private void notifyOnLeadershipAcquired() {
		try {
			listener.onLeadershipAcquired();
		} catch (Exception e) {
			LOG.error("Listener failed on leadership acquired notification", e);
		}
	}

	private void notifyOnLeadershipRevoked() {
		try {
			listener.onLeadershipRevoked();
		} catch (Exception e) {
			LOG.error("Listener failed on leadership revoked notification", e);
		}
	}

	private void notifyOnLeaderResolved(ConsulLeaderData data) {
		try {
			listener.onLeaderResolved(data.getAddress(), data.getSessionId());
		} catch (Exception e) {
			LOG.error("Listener failed on leadership revoked notification", e);
		}
	}

}
