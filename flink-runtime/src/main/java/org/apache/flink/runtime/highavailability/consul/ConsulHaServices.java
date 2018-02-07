package org.apache.flink.runtime.highavailability.consul;

import com.ecwid.consul.v1.ConsulClient;
import org.apache.flink.api.common.JobID;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.blob.BlobStore;
import org.apache.flink.runtime.blob.BlobStoreService;
import org.apache.flink.runtime.checkpoint.CheckpointRecoveryFactory;
import org.apache.flink.runtime.highavailability.HighAvailabilityServices;
import org.apache.flink.runtime.highavailability.RunningJobsRegistry;
import org.apache.flink.runtime.jobmanager.SubmittedJobGraphStore;
import org.apache.flink.runtime.leaderelection.LeaderElectionService;
import org.apache.flink.runtime.leaderretrieval.LeaderRetrievalService;

import java.io.IOException;
import java.util.concurrent.Executor;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * An implementation of {@link HighAvailabilityServices} using Hashicorp Consul.
 */
public class ConsulHaServices implements HighAvailabilityServices {

	private static final String RESOURCE_MANAGER_LEADER_PATH = "/resource_manager_lock";

	private static final String DISPATCHER_LEADER_PATH = "/dispatcher_lock";

	private static final String JOB_MANAGER_LEADER_PATH = "/job_manager_lock";

	/**
	 * Consul client to use
	 */
	private final ConsulClient client;

	/**
	 * The executor to run Consul callbacks on
	 */
	private final Executor executor;

	/**
	 * The runtime configuration
	 */
	private final Configuration configuration;

	/**
	 * The Consul based running jobs registry
	 */
	private final RunningJobsRegistry runningJobsRegistry;

	/**
	 * Store for arbitrary blobs
	 */
	private final BlobStoreService blobStoreService;

	public ConsulHaServices(ConsulClient client,
							Executor executor,
							Configuration configuration,
							BlobStoreService blobStoreService) {
		this.client = checkNotNull(client);
		this.executor = checkNotNull(executor);
		this.configuration = checkNotNull(configuration);

		this.runningJobsRegistry = null;

		this.blobStoreService = checkNotNull(blobStoreService);
	}

	@Override
	public LeaderRetrievalService getResourceManagerLeaderRetriever() {
		return null;
	}

	@Override
	public LeaderRetrievalService getDispatcherLeaderRetriever() {
		return null;
	}

	@Override
	public LeaderRetrievalService getJobManagerLeaderRetriever(JobID jobID) {
		return null;
	}

	@Override
	public LeaderRetrievalService getJobManagerLeaderRetriever(JobID jobID, String defaultJobManagerAddress) {
		return null;
	}

	@Override
	public LeaderElectionService getResourceManagerLeaderElectionService() {
		return null;
	}

	@Override
	public LeaderElectionService getDispatcherLeaderElectionService() {
		return null;
	}

	@Override
	public LeaderElectionService getJobManagerLeaderElectionService(JobID jobID) {
		return null;
	}

	@Override
	public CheckpointRecoveryFactory getCheckpointRecoveryFactory() {
		return null;
	}

	@Override
	public SubmittedJobGraphStore getSubmittedJobGraphStore() throws Exception {
		return null;
	}

	@Override
	public RunningJobsRegistry getRunningJobsRegistry() throws Exception {
		return null;
	}

	@Override
	public BlobStore createBlobStore() throws IOException {
		return null;
	}

	@Override
	public void close() throws Exception {

	}

	@Override
	public void closeAndCleanupAllData() throws Exception {

	}
}
