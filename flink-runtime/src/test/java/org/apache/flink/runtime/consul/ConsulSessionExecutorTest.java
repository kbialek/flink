package org.apache.flink.runtime.consul;

import com.ecwid.consul.v1.ConsulClient;
import com.ecwid.consul.v1.QueryParams;
import com.ecwid.consul.v1.session.model.NewSession;
import com.pszymczyk.consul.ConsulProcess;
import com.pszymczyk.consul.ConsulStarterBuilder;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.concurrent.Executor;
import java.util.concurrent.Executors;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.mockito.Mockito.*;

public class ConsulSessionExecutorTest {

	private ConsulProcess consul;
	private ConsulClient client;

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
	public void testSessionLifecycle() throws Exception {
		Executor executor = Executors.newFixedThreadPool(2);
		ConsulClient spiedClient = spy(client);
		ConsulSessionExecutor cse = new ConsulSessionExecutor(spiedClient, executor, 10);
		cse.start();
		verify(spiedClient).sessionCreate(any(NewSession.class), any(QueryParams.class));

		final String[] sessionId = new String[1];

		cse.execute((c, sid) -> {
			sessionId[0] = sid;
			assertNotNull(c);
			assertNotNull(sid);
			assertNotNull(c.getSessionInfo(sid, QueryParams.DEFAULT).getValue());
			try {
				Thread.sleep(11000);
			} catch (InterruptedException ignored) {

			}
			verify(spiedClient).renewSession(anyString(), any(QueryParams.class));
			assertNotNull(c.getSessionInfo(sid, QueryParams.DEFAULT).getValue());
		});
		Thread.sleep(20000);
		verify(spiedClient).sessionDestroy(anyString(), any(QueryParams.class));
		assertNull(client.getSessionInfo(sessionId[0], QueryParams.DEFAULT).getValue());
	}

}
