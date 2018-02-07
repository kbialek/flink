package org.apache.flink.runtime.consul;

import com.ecwid.consul.v1.ConsulClient;
import com.ecwid.consul.v1.QueryParams;
import com.ecwid.consul.v1.Response;
import com.ecwid.consul.v1.kv.model.GetBinaryValue;
import com.ecwid.consul.v1.session.model.NewSession;
import com.google.common.base.Preconditions;
import org.apache.flink.runtime.leaderretrieval.LeaderRetrievalListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.Executor;

public final class ConsulLeaderRetriever {

	private static final Logger LOG = LoggerFactory.getLogger(ConsulLeaderRetriever.class);

	private final ConsulClient client;

	private final Executor executor;

	private final String leaderKey;

	/**
	 * SessionID used for communication with Consul
	 */
	private String consulSessionId;

	private long leaderKeyIndex;

	private volatile boolean runnable;

	private ConsulLeaderData leaderData;

	private final LeaderRetrievalListener listener;

	private final int waitTime;

	/**
	 * @param client    Consul client
	 * @param executor  Executor to run background tasks
	 * @param leaderKey key in Consul KV store
	 * @param waitTime  Consul blocking read timeout (in seconds)
	 */
	public ConsulLeaderRetriever(ConsulClient client,
								 Executor executor,
								 String leaderKey,
								 LeaderRetrievalListener listener,
								 int waitTime) {
		this.client = Preconditions.checkNotNull(client, "client");
		this.executor = Preconditions.checkNotNull(executor, "executor");
		this.leaderKey = Preconditions.checkNotNull(leaderKey, "leaderKey");
		this.listener = Preconditions.checkNotNull(listener, "listener");
		this.waitTime = waitTime;
	}

	public void start() {
		LOG.info("Starting Consul Leader Resolver");
		runnable = true;
		executor.execute(this::watch);
	}

	public void stop() {
		LOG.info("Stopping Consul Leader Resolver");
		runnable = false;
	}

	private void watch() {
		while (runnable) {
			try {
				createConsulSessionIfNecessary();
				GetBinaryValue value = readLeaderKey();
				String leaderSessionId = null;
				if (value != null) {
					leaderKeyIndex = value.getModifyIndex();
					leaderSessionId = value.getSession();
				}

				if (runnable && leaderSessionId != null) {
					leaderResolved(ConsulLeaderData.from(value.getValue()));
				}
			} catch (Exception exception) {
				listener.handleError(exception);
				// backoff
				try {
					Thread.sleep(waitTime * 1000);
				} catch (InterruptedException ignored) {

				}
			}
		}
		destroyConsulSession();
	}

	private void createConsulSessionIfNecessary() {
		if (consulSessionId == null) {
			NewSession newSession = new NewSession();
			newSession.setName("flink");
			consulSessionId = client.sessionCreate(newSession, QueryParams.DEFAULT).getValue();
		}
	}

	private void destroyConsulSession() {
		try {
			client.sessionDestroy(consulSessionId, QueryParams.DEFAULT);
		} catch (Exception e) {
			LOG.error("Consul session destroy failed", e);
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

	private void leaderResolved(ConsulLeaderData data) {
		if (!data.equals(leaderData)) {
			leaderData = data;
			notifyOnLeaderResolved(data);
			LOG.info("Cluster hasLeadership resolved {}", data);
		}
	}

	private void notifyOnLeaderResolved(ConsulLeaderData data) {
		try {
			listener.notifyLeaderAddress(data.getAddress(), data.getSessionId());
		} catch (Exception e) {
			LOG.error("Listener failed on leadership revoked notification", e);
		}
	}

}
