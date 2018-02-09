package org.apache.flink.runtime.consul;

import com.ecwid.consul.v1.ConsulClient;
import org.apache.flink.api.common.JobID;
import org.apache.flink.runtime.checkpoint.CheckpointIDCounter;
import org.apache.flink.runtime.checkpoint.CheckpointRecoveryFactory;
import org.apache.flink.runtime.checkpoint.CompletedCheckpointStore;
import org.apache.flink.util.Preconditions;

public final class ConsulCheckpointRecoveryFactory implements CheckpointRecoveryFactory {

	private final ConsulClient client;

	public ConsulCheckpointRecoveryFactory(ConsulClient client) {
		this.client = Preconditions.checkNotNull(client, "client");
	}

	@Override
	public CompletedCheckpointStore createCheckpointStore(JobID jobId, int maxNumberOfCheckpointsToRetain, ClassLoader userClassLoader) throws Exception {
		return new ConsulCompletedCheckpointStore(client, jobId, maxNumberOfCheckpointsToRetain);
	}

	@Override
	public CheckpointIDCounter createCheckpointIDCounter(JobID jobId) throws Exception {
		return new ConsulCheckpointIDCounter(client, jobId);
	}
}
