package org.apache.flink.runtime.consul;

import com.ecwid.consul.v1.ConsulClient;
import com.ecwid.consul.v1.OperationException;
import com.ecwid.consul.v1.QueryParams;
import com.ecwid.consul.v1.Response;
import com.ecwid.consul.v1.kv.model.GetValue;
import com.ecwid.consul.v1.kv.model.PutParams;
import com.ecwid.consul.v1.session.model.NewSession;
import com.google.common.base.Preconditions;
import org.apache.flink.runtime.leaderelection.LeaderContender;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.UUID;
import java.util.concurrent.Executor;

public final class ConsulLeadershipLatch {

	private static final Logger LOG = LoggerFactory.getLogger(ConsulLeadershipLatch.class);


	private final ConsulClient client;

	private final Executor executor;

	private final String leaderKey;

	private final LeaderContender leaderContender;

	private String currentNodeSessionId;

	private long leaderKeyIndex;

	private volatile boolean runnable;

	private boolean leader;

	private final int waitTime;

	/**
	 * @param client Consul client
	 * @param executor Executor to run background tasks
	 * @param leaderKey key in Consul KV store
	 * @param leaderContender leadership changes are reported to this contender
	 * @param waitTime Consul blocking read timeout (in seconds)
	 */
	public ConsulLeadershipLatch(ConsulClient client,
								 Executor executor,
								 String leaderKey,
								 LeaderContender leaderContender,
								 int waitTime) {
		this.client = Preconditions.checkNotNull(client, "client");
		this.executor = Preconditions.checkNotNull(executor, "executor");
		this.leaderKey = Preconditions.checkNotNull(leaderKey, "leaderKey");
		this.leaderContender = Preconditions.checkNotNull(leaderContender, "leaderContender");
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
		leaderContender.revokeLeadership();
		client.sessionDestroy(currentNodeSessionId, QueryParams.DEFAULT);
	}

	private void watch() {
		while (runnable) {
			createConsulSessionIfNecessary();
			GetValue value = readLeaderKey();
			String leaderSessionId = null;
			if (value != null) {
				leaderKeyIndex = value.getModifyIndex();
				leaderSessionId = value.getSession();
			}

			if (runnable) {
				if (leaderSessionId == null) {
					LOG.info("No leader elected. Current node is trying to register");
					Boolean success = writeLeaderKey();
					if (success) {
						leaderContender.grantLeadership(UUID.randomUUID());
						leader = true;
						LOG.info("Current node is now a leader");
					} else {
						leaderContender.revokeLeadership();
						leader = false;
					}
				}
			}
		}
	}

	private void createConsulSessionIfNecessary() {
		if (currentNodeSessionId == null) {
			NewSession newSession = new NewSession();
			newSession.setName("flink");
			currentNodeSessionId = client.sessionCreate(newSession, QueryParams.DEFAULT).getValue();
		}
	}

	private GetValue readLeaderKey() {
		QueryParams queryParams = QueryParams.Builder.builder()
			.setIndex(leaderKeyIndex)
			.setWaitTime(waitTime)
			.build();
		Response<GetValue> leaderKeyValue = client.getKVValue(leaderKey, queryParams);
		return leaderKeyValue.getValue();
	}

	private Boolean writeLeaderKey() {
		PutParams putParams = new PutParams();
		putParams.setAcquireSession(currentNodeSessionId);
		try {
			return client.setKVValue(leaderKey, leaderContender.getAddress(), putParams).getValue();
		} catch (OperationException ex) {
			return false;
		}
	}


}
