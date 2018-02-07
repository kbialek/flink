package org.apache.flink.runtime.leaderretrieval;

import com.ecwid.consul.v1.ConsulClient;
import org.apache.flink.runtime.consul.ConsulLeaderRetriever;
import org.apache.flink.util.Preconditions;

import java.util.concurrent.Executor;

public final class ConsulLeaderRetrievalService implements LeaderRetrievalService {

	private final Object lock = new Object();

	private final ConsulClient client;
	private final Executor executor;
	private final String leaderKey;

	private ConsulLeaderRetriever leaderRetriever;

	public ConsulLeaderRetrievalService(ConsulClient client,
										Executor executor,
										String leaderKey) {
		this.client = Preconditions.checkNotNull(client, "client");
		this.executor = Preconditions.checkNotNull(executor, "executor");
		this.leaderKey = Preconditions.checkNotNull(leaderKey, "leaderKey");
	}

	@Override
	public void start(LeaderRetrievalListener listener) throws Exception {
		Preconditions.checkState(leaderRetriever == null, "ConsulLeaderRetrievalService is already started");
		synchronized (lock) {
			this.leaderRetriever = new ConsulLeaderRetriever(client, executor, leaderKey, listener, 10);
		}
	}

	@Override
	public void stop() throws Exception {
		synchronized (lock) {
			if (this.leaderRetriever != null) {
				this.leaderRetriever.stop();
			}
		}
	}
}
