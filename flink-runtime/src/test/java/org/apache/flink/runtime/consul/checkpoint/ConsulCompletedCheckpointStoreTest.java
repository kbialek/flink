package org.apache.flink.runtime.consul.checkpoint;

import com.ecwid.consul.v1.ConsulClient;
import com.pszymczyk.consul.ConsulProcess;
import com.pszymczyk.consul.ConsulStarterBuilder;
import org.apache.flink.api.common.JobID;
import org.apache.flink.runtime.checkpoint.CheckpointProperties;
import org.apache.flink.runtime.checkpoint.CompletedCheckpoint;
import org.apache.flink.runtime.jobgraph.JobStatus;
import org.apache.flink.runtime.state.StreamStateHandle;
import org.apache.flink.runtime.state.memory.ByteStreamStateHandle;
import org.apache.flink.runtime.zookeeper.RetrievableStateStorageHelper;
import org.apache.flink.runtime.zookeeper.filesystem.FileSystemStateStorageHelper;
import org.apache.flink.shaded.guava18.com.google.common.collect.Lists;
import org.apache.flink.shaded.guava18.com.google.common.collect.Maps;
import org.apache.flink.shaded.guava18.com.google.common.io.Files;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.List;

import static org.junit.Assert.*;

public class ConsulCompletedCheckpointStoreTest {

	private ConsulProcess consul;
	private ConsulClient client;
	private File tempDir;
	private RetrievableStateStorageHelper<CompletedCheckpoint> storage;

	@Before
	public void setup() throws IOException {
		consul = ConsulStarterBuilder.consulStarter()
			.withConsulVersion("1.0.3")
			.build()
			.start();
		client = new ConsulClient(String.format("localhost:%d", consul.getHttpPort()));
		tempDir = Files.createTempDir();
		storage = new FileSystemStateStorageHelper<CompletedCheckpoint>(tempDir.getPath(), "cp");
	}

	@After
	public void cleanup() {
		consul.close();
	}

	@Test
	public void testConfiguration() throws Exception {
		JobID jobID = JobID.generate();
		ConsulCompletedCheckpointStore store = new ConsulCompletedCheckpointStore(client, jobID, 10, storage);
		assertEquals(10, store.getMaxNumberOfRetainedCheckpoints());
		assertTrue(store.requiresExternalizedCheckpoints());
	}

	@Test
	public void testAddCheckpoint() throws Exception {
		JobID jobID = JobID.generate();
		ConsulCompletedCheckpointStore store = new ConsulCompletedCheckpointStore(client, jobID, 1, storage);

		CompletedCheckpoint checkpoint = createCheckpoint(jobID, 1l);

		assertEquals(0, store.getNumberOfRetainedCheckpoints());

		store.addCheckpoint(checkpoint);

		assertEquals(1, store.getNumberOfRetainedCheckpoints());

		CompletedCheckpoint latestCheckpoint = store.getLatestCheckpoint();
		assertNotNull(latestCheckpoint);
		assertEquals(checkpoint, latestCheckpoint);
		assertSame(checkpoint, latestCheckpoint);
	}

	@Test
	public void testAllCheckpoints() throws Exception {
		JobID jobID = JobID.generate();
		ConsulCompletedCheckpointStore store = new ConsulCompletedCheckpointStore(client, jobID, 3, storage);

		CompletedCheckpoint checkpoint1 = createCheckpoint(jobID, 1l);
		CompletedCheckpoint checkpoint2 = createCheckpoint(jobID, 2l);

		store.addCheckpoint(checkpoint1);
		store.addCheckpoint(checkpoint2);

		List<CompletedCheckpoint> allCheckpoints = store.getAllCheckpoints();
		assertNotNull(allCheckpoints);
		assertEquals(2, allCheckpoints.size());
		assertEquals(checkpoint1, allCheckpoints.get(0));
		assertSame(checkpoint1, allCheckpoints.get(0));
		assertEquals(checkpoint2, allCheckpoints.get(1));
		assertSame(checkpoint2, allCheckpoints.get(1));
	}

	@Test
	public void testRecovery() throws Exception {
		JobID jobID = JobID.generate();
		ConsulCompletedCheckpointStore store = new ConsulCompletedCheckpointStore(client, jobID, 1, storage);

		CompletedCheckpoint checkpoint = createCheckpoint(jobID, 1l);
		store.addCheckpoint(checkpoint);

		ConsulCompletedCheckpointStore newStore = new ConsulCompletedCheckpointStore(client, jobID, 1, storage);
		newStore.recover();

		CompletedCheckpoint latestCheckpoint = newStore.getLatestCheckpoint();
		assertNotNull(latestCheckpoint);
		assertEquals(checkpoint, latestCheckpoint);
		assertNotSame(checkpoint, latestCheckpoint);
	}

	@Test
	public void testShutdown() throws Exception {
		JobID jobID = JobID.generate();
		ConsulCompletedCheckpointStore store = new ConsulCompletedCheckpointStore(client, jobID, 1, storage);

		CompletedCheckpoint checkpoint = createCheckpoint(jobID, 1l);
		store.addCheckpoint(checkpoint);
		store.shutdown(JobStatus.FINISHED);

		ConsulCompletedCheckpointStore newStore = new ConsulCompletedCheckpointStore(client, jobID, 1, storage);
		newStore.recover();

		assertEquals(0, newStore.getAllCheckpoints().size());
	}

	@Test
	public void testRemoveSubsumedCheckpoint() throws Exception {
		JobID jobID = JobID.generate();
		ConsulCompletedCheckpointStore store = new ConsulCompletedCheckpointStore(client, jobID, 1, storage);

		CompletedCheckpoint checkpoint1 = createCheckpoint(jobID, 1l);
		CompletedCheckpoint checkpoint2 = createCheckpoint(jobID, 2l);

		store.addCheckpoint(checkpoint1);
		store.addCheckpoint(checkpoint2);

		CompletedCheckpoint latestCheckpoint = store.getLatestCheckpoint();
		assertNotNull(latestCheckpoint);
		assertEquals(checkpoint2, latestCheckpoint);
		assertSame(checkpoint2, latestCheckpoint);

		assertEquals(1, store.getAllCheckpoints().size());
	}

	private CompletedCheckpoint createCheckpoint(JobID jobID, long checkpointId) {
		long timestamp = checkpointId;
		long completionTimestamp = checkpointId;
		CheckpointProperties chkProps = CheckpointProperties.forStandardCheckpoint();
		StreamStateHandle handle = new ByteStreamStateHandle("handle", new byte[]{});
		return new CompletedCheckpoint(jobID, checkpointId, timestamp, completionTimestamp, Maps.newHashMap(),
			Lists.newArrayList(), chkProps, handle, "");
	}

}
