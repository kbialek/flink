package org.apache.flink.runtime.consul;

import com.ecwid.consul.v1.ConsulClient;
import com.ecwid.consul.v1.kv.model.GetValue;
import com.ecwid.consul.v1.kv.model.PutParams;
import org.apache.flink.api.common.JobID;
import org.apache.flink.runtime.highavailability.RunningJobsRegistry;
import org.apache.flink.util.Preconditions;

import java.io.IOException;

public final class ConsulRunningJobsRegistry implements RunningJobsRegistry {

	private final ConsulClient client;
	private final ConsulSessionHolder sessionHolder;

	public ConsulRunningJobsRegistry(ConsulClient client, ConsulSessionHolder sessionHolder) {
		this.client = Preconditions.checkNotNull(client, "client");
		this.sessionHolder = Preconditions.checkNotNull(sessionHolder, "sessionHolder");
	}

	@Override
	public void setJobRunning(JobID jobID) throws IOException {
		storeJobStatus(jobID, JobSchedulingStatus.RUNNING);
	}

	@Override
	public void setJobFinished(JobID jobID) throws IOException {
		storeJobStatus(jobID, JobSchedulingStatus.DONE);
	}

	@Override
	public JobSchedulingStatus getJobSchedulingStatus(JobID jobID) throws IOException {
		GetValue value = client.getKVValue(path(jobID)).getValue();
		return value == null ? JobSchedulingStatus.PENDING : JobSchedulingStatus.valueOf(value.getDecodedValue());
	}

	@Override
	public void clearJob(JobID jobID) throws IOException {
		client.deleteKVValue(path(jobID));
	}

	private void storeJobStatus(JobID jobID, JobSchedulingStatus status) {
		PutParams params = new PutParams();
		params.setAcquireSession(sessionHolder.getSessionId());
		if (!client.setKVValue(path(jobID), status.name(), params).getValue()) {
			throw new IllegalStateException(String.format("Failed to store JobStatus(%s) for JobID: %s", status.name(), jobID.toString()));
		}
	}

	private String path(JobID jobID) {
		return "flink/job-status/" + jobID.toString();
	}
}
