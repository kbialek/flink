package org.apache.flink.runtime.consul;

import com.ecwid.consul.v1.ConsulClient;
import com.ecwid.consul.v1.kv.model.GetBinaryValue;
import org.apache.flink.api.common.JobID;
import org.apache.flink.runtime.jobmanager.SubmittedJobGraph;
import org.apache.flink.runtime.jobmanager.SubmittedJobGraphStore;
import org.apache.flink.util.FlinkException;
import org.apache.flink.util.InstantiationUtil;
import org.apache.flink.util.Preconditions;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

public final class ConsulSubmittedJobGraphStore implements SubmittedJobGraphStore {

	private static final String FLINK_JOBGRAPHS_PATH = "flink/jobgraphs/";
	private final ConsulClient client;
	private SubmittedJobGraphListener listener;

	public ConsulSubmittedJobGraphStore(ConsulClient client) {
		this.client = client;
	}

	@Override
	public void start(SubmittedJobGraphListener jobGraphListener) throws Exception {
		this.listener = Preconditions.checkNotNull(jobGraphListener, "jobGraphListener");
	}

	@Override
	public void stop() throws Exception {

	}

	@Override
	public void putJobGraph(SubmittedJobGraph jobGraph) throws Exception {
		byte[] bytes = InstantiationUtil.serializeObject(jobGraph);
		client.setKVBinaryValue(path(jobGraph.getJobId()), bytes);
		this.listener.onAddedJobGraph(jobGraph.getJobId());
	}

	@Override
	public SubmittedJobGraph recoverJobGraph(JobID jobId) throws Exception {
		GetBinaryValue value = client.getKVBinaryValue(path(jobId)).getValue();
		if (value != null) {
			try {
				return InstantiationUtil.deserializeObject(value.getValue(), Thread.currentThread().getContextClassLoader());
			} catch (Exception e) {
				throw new FlinkException("Could not deserialize SubmittedJobGraph for Job " + jobId.toString(), e);
			}
		} else {
			throw new FlinkException("Could not retrieve SubmittedJobGraph for Job " + jobId.toString());
		}
	}

	@Override
	public void removeJobGraph(JobID jobId) throws Exception {
		client.deleteKVValue(path(jobId));
		listener.onRemovedJobGraph(jobId);
	}

	@Override
	public Collection<JobID> getJobIds() throws Exception {
		List<String> value = client.getKVKeysOnly(FLINK_JOBGRAPHS_PATH).getValue();
		if (value != null) {
			return value.stream()
				.map(id -> id.split("/"))
				.map(parts -> parts[parts.length - 1])
				.map(JobID::fromHexString).collect(Collectors.toList());
		}
		return Collections.emptyList();
	}

	private String path(JobID jobID) {
		return FLINK_JOBGRAPHS_PATH + jobID.toString();
	}
}
