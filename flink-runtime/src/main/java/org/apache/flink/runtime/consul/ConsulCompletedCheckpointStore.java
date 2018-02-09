package org.apache.flink.runtime.consul;

import com.ecwid.consul.v1.ConsulClient;
import com.ecwid.consul.v1.kv.model.GetBinaryValue;
import org.apache.curator.shaded.com.google.common.collect.Lists;
import org.apache.flink.api.common.JobID;
import org.apache.flink.runtime.checkpoint.CompletedCheckpoint;
import org.apache.flink.runtime.checkpoint.CompletedCheckpointStore;
import org.apache.flink.runtime.jobgraph.JobStatus;
import org.apache.flink.runtime.state.RetrievableStateHandle;
import org.apache.flink.runtime.zookeeper.RetrievableStateStorageHelper;
import org.apache.flink.util.InstantiationUtil;
import org.apache.flink.util.Preconditions;

import java.io.IOException;
import java.util.ArrayDeque;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;

public final class ConsulCompletedCheckpointStore implements CompletedCheckpointStore {

	private final ConsulClient client;
	private JobID jobID;
	private final int maxCheckpoints;
	private final RetrievableStateStorageHelper<CompletedCheckpoint> storage;

	private final ArrayDeque<CompletedCheckpoint> completedCheckpoints;

	public ConsulCompletedCheckpointStore(ConsulClient client, JobID jobID, int maxCheckpoints,
										  RetrievableStateStorageHelper<CompletedCheckpoint> storage) {
		this.client = Preconditions.checkNotNull(client, "client");
		this.jobID = Preconditions.checkNotNull(jobID, "jobID");
		this.storage = Preconditions.checkNotNull(storage, "storage");
		Preconditions.checkState(maxCheckpoints > 0, "maxCheckpoints must be > 0");
		this.maxCheckpoints = maxCheckpoints;

		this.completedCheckpoints = new ArrayDeque<>(maxCheckpoints + 1);
	}

	@Override
	public void addCheckpoint(CompletedCheckpoint checkpoint) throws Exception {
		writeCheckpoint(checkpoint);
		completedCheckpoints.add(checkpoint);

		if (completedCheckpoints.size() > maxCheckpoints) {
			removeCheckpoint(completedCheckpoints.removeFirst());
		}
	}

	@Override
	public List<CompletedCheckpoint> getAllCheckpoints() throws Exception {
		return Lists.newArrayList(completedCheckpoints);
	}

	@Override
	public CompletedCheckpoint getLatestCheckpoint() throws Exception {
		return completedCheckpoints.peekLast();
	}

	@Override
	public void recover() throws Exception {
		List<String> checkpointPaths = client.getKVKeysOnly(jobPath()).getValue();
		if (checkpointPaths != null) {
			checkpointPaths.sort(Comparator.naturalOrder());
			checkpointPaths.stream().map(path -> {
				try {
					GetBinaryValue binaryValue = client.getKVBinaryValue(path).getValue();

					RetrievableStateHandle<CompletedCheckpoint> retrievableStateHandle =
						InstantiationUtil.deserializeObject(
							binaryValue.getValue(),
							Thread.currentThread().getContextClassLoader()
						);

					return retrievableStateHandle.retrieveState();
				} catch (IOException | ClassNotFoundException e) {
					throw new IllegalStateException(e);
				}
			}).collect(Collectors.toCollection(() -> completedCheckpoints));
		}
	}

	@Override
	public void shutdown(JobStatus jobStatus) throws Exception {
		completedCheckpoints.forEach(this::removeCheckpoint);
	}

	@Override
	public int getNumberOfRetainedCheckpoints() {
		return completedCheckpoints.size();
	}

	@Override
	public int getMaxNumberOfRetainedCheckpoints() {
		return maxCheckpoints;
	}

	@Override
	public boolean requiresExternalizedCheckpoints() {
		return true;
	}

	private String jobPath() {
		return "flink/checkpoints/" + jobID.toString();
	}

	private void writeCheckpoint(CompletedCheckpoint checkpoint) throws Exception {
		String key = jobPath() + checkpoint.getCheckpointID();

		RetrievableStateHandle<CompletedCheckpoint> storeHandle = storage.store(checkpoint);

		byte[] serializedStoreHandle = InstantiationUtil.serializeObject(storeHandle);
		boolean success = false;
		try {
			success = client.setKVBinaryValue(key, serializedStoreHandle).getValue();
		} catch (Exception ignored) {

		}
		if (!success) {
			// cleanup if data was not stored in Consul
			if (storeHandle != null) {
				storeHandle.discardState();
			}
		}
	}

	private void removeCheckpoint(CompletedCheckpoint checkpoint) {
		String key = jobPath() + checkpoint.getCheckpointID();
		client.deleteKVValue(key);
	}


}
