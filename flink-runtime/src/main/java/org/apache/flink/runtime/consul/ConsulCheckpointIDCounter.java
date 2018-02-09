package org.apache.flink.runtime.consul;

import com.ecwid.consul.v1.ConsulClient;
import com.ecwid.consul.v1.kv.model.GetValue;
import com.ecwid.consul.v1.kv.model.PutParams;
import org.apache.flink.api.common.JobID;
import org.apache.flink.runtime.checkpoint.CheckpointIDCounter;
import org.apache.flink.runtime.jobgraph.JobStatus;
import org.apache.flink.util.Preconditions;

public final class ConsulCheckpointIDCounter implements CheckpointIDCounter {

	private final ConsulClient client;
	private final JobID jobID;
	private long index;

	public ConsulCheckpointIDCounter(ConsulClient client, JobID jobID) {
		this.client = Preconditions.checkNotNull(client, "client");
		this.jobID = Preconditions.checkNotNull(jobID, "jobID");
	}

	@Override
	public void start() throws Exception {

	}

	@Override
	public void shutdown(JobStatus jobStatus) throws Exception {
		if (jobStatus.isGloballyTerminalState()) {
			removeCounter();
		}
	}

	@Override
	public long getAndIncrement() throws Exception {
		while (true) {
			long v = readCounter();
			if (writeCounter(v + 1)) {
				return v;
			} else {
				Thread.sleep(100);
			}
		}
	}

	@Override
	public void setCount(long newId) throws Exception {
		writeCounter(newId);
	}

	private long readCounter() {
		GetValue gv = client.getKVValue(counterKey()).getValue();
		if (gv == null) {
			index = 0;
			return 0;
		} else {
			index = gv.getModifyIndex();
			return Long.valueOf(gv.getDecodedValue());
		}
	}

	private boolean writeCounter(long value) {
		PutParams params = new PutParams();
		params.setCas(index);
		return client.setKVValue(counterKey(), String.valueOf(value), params).getValue();
	}

	private void removeCounter() {
		client.deleteKVValue(counterKey());
	}

	private String counterKey() {
		return "flink/checkpoint-counter/" + jobID.toString();
	}
}
