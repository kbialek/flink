package org.apache.flink.runtime.consul;

import com.ecwid.consul.v1.ConsulClient;
import com.pszymczyk.consul.ConsulProcess;
import com.pszymczyk.consul.ConsulStarterBuilder;
import org.apache.flink.runtime.leaderelection.LeaderContender;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.UUID;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;

import static org.mockito.Mockito.*;

public class ConsulLeadershipLatchTest {

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

		LeaderContender leaderContender = mock(LeaderContender.class);

		when(leaderContender.getAddress()).thenReturn("leader-address");

		ConsulLeadershipLatch latch = new ConsulLeadershipLatch(client, executor, leaderKey, leaderContender, waitTime);
		latch.start();

		Thread.sleep(1000 * waitTime);
		verify(leaderContender).grantLeadership(any(UUID.class));

		latch.stop();
	}

	@Test
	public void testLeaderElectionTwoNodes() throws InterruptedException {
		String leaderKey = "test-key";

		LeaderContender leaderContender1 = mock(LeaderContender.class);
		LeaderContender leaderContender2 = mock(LeaderContender.class);

		when(leaderContender1.getAddress()).thenReturn("leader-address1");
		when(leaderContender2.getAddress()).thenReturn("leader-address2");

		ConsulLeadershipLatch latch1 = new ConsulLeadershipLatch(client, executor, leaderKey, leaderContender1, waitTime);
		ConsulLeadershipLatch latch2 = new ConsulLeadershipLatch(client, executor, leaderKey, leaderContender2, waitTime);

		latch1.start();
		Thread.sleep(100);
		latch2.start();

		Thread.sleep(2000 * waitTime);
		verify(leaderContender1).grantLeadership(any(UUID.class));

		latch1.stop();
		latch2.stop();
	}

	@Test
	public void testConsulReset() throws InterruptedException {
		String leaderKey = "test-key";

		LeaderContender leaderContender = mock(LeaderContender.class);

		when(leaderContender.getAddress()).thenReturn("leader-address");

		ConsulLeadershipLatch latch = new ConsulLeadershipLatch(client, executor, leaderKey, leaderContender, waitTime);
		latch.start();

		Thread.sleep(1000 * waitTime);
		verify(leaderContender).grantLeadership(any(UUID.class));

		consul.reset();
		Thread.sleep(1000 * waitTime);
		verify(leaderContender).revokeLeadership();

		latch.stop();
	}

}
