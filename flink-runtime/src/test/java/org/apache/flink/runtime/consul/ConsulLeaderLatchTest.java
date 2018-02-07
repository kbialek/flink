package org.apache.flink.runtime.consul;

import com.ecwid.consul.v1.ConsulClient;
import com.pszymczyk.consul.ConsulProcess;
import com.pszymczyk.consul.ConsulStarterBuilder;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.UUID;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;

import static org.mockito.Mockito.*;

public class ConsulLeaderLatchTest {

	private ConsulProcess consul;
	private ConsulClient client;
	private Executor executor = Executors.newFixedThreadPool(4);
	private int waitTime = 1;

	@Before
	public void setup() {
		consul = ConsulStarterBuilder.consulStarter()
			.withConsulVersion("1.0.3")
			.build()
			.start();
		client = new ConsulClient(String.format("localhost:%d", consul.getHttpPort()));
	}

	@After
	public void cleanup() {
		consul.close();
	}

	@Test
	public void testLeaderElection() throws InterruptedException {
		String leaderKey = "test-key";

		ConsulLeaderLatchListener listener = mock(ConsulLeaderLatchListener.class);

		ConsulLeaderLatch latch = new ConsulLeaderLatch(client, executor, leaderKey, "leader-address", listener, waitTime);
		latch.start();

		Thread.sleep(1000 * waitTime);
		verify(listener).onLeadershipAcquired();

		latch.stop();
	}

	@Test
	public void testLeaderElectionTwoNodes() throws InterruptedException {
		String leaderKey = "test-key";

		ConsulLeaderLatchListener listener1 = mock(ConsulLeaderLatchListener.class);
		ConsulLeaderLatchListener listener2 = mock(ConsulLeaderLatchListener.class);

		ConsulLeaderLatch latch1 = new ConsulLeaderLatch(client, executor, leaderKey, "leader-address1", listener1, waitTime);
		ConsulLeaderLatch latch2 = new ConsulLeaderLatch(client, executor, leaderKey, "leader-address2", listener2, waitTime);

		latch1.start();
		Thread.sleep(100);
		latch2.start();

		Thread.sleep(2000 * waitTime);
		verify(listener1).onLeadershipAcquired();
		verify(listener2).onLeaderResolved(eq("leader-address1"), any(UUID.class));

		latch1.stop();
		Thread.sleep(2000 * waitTime);
		verify(listener2).onLeadershipAcquired();

		latch2.stop();
	}

	@Test
	public void testConsulReset() throws InterruptedException {
		String leaderKey = "test-key";

		ConsulLeaderLatchListener listener = mock(ConsulLeaderLatchListener.class);

		ConsulLeaderLatch latch = new ConsulLeaderLatch(client, executor, leaderKey, "leader-address", listener, waitTime);
		latch.start();

		Thread.sleep(1000 * waitTime);
		verify(listener).onLeadershipAcquired();

		consul.reset();
		Thread.sleep(1000 * waitTime);
		verify(listener).onLeadershipRevoked();

		latch.stop();
	}

}
