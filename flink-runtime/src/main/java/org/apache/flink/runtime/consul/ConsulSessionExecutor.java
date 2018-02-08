package org.apache.flink.runtime.consul;

import com.ecwid.consul.v1.ConsulClient;
import com.ecwid.consul.v1.QueryParams;
import com.ecwid.consul.v1.session.model.NewSession;
import org.apache.flink.util.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicInteger;

public final class ConsulSessionExecutor {

	private static final Logger LOG = LoggerFactory.getLogger(ConsulSessionExecutor.class);

	private final Object lock = new Object();

	private final ConsulClient client;
	private final Executor executor;
	private final int sessionTtl;
	private final AtomicInteger taskCounter = new AtomicInteger(0);
	private volatile String consulSessionId;
	private volatile boolean running;

	public ConsulSessionExecutor(ConsulClient client, Executor executor, int sessionTtl) {
		this.client = Preconditions.checkNotNull(client, "client");
		this.executor = Preconditions.checkNotNull(executor, "executor");
		this.sessionTtl = sessionTtl;
	}

	public void execute(ConsulTask task) {
		synchronized (lock) {
			taskCounter.incrementAndGet();
			executor.execute(new TaskDecorator(task));
		}
	}

	private void createOrRenewConsulSession() {
		if (consulSessionId == null) {
			createConsulSession();
		} else {
			renewConsulSession();
		}
	}

	private void createConsulSession() {
		NewSession newSession = new NewSession();
		newSession.setName("flink");
		newSession.setTtl(String.format("%ds", Math.max(10, sessionTtl)));
		consulSessionId = client.sessionCreate(newSession, QueryParams.DEFAULT).getValue();
	}

	private void renewConsulSession() {
		try {
			client.renewSession(consulSessionId, QueryParams.DEFAULT);
		} catch (Exception e) {
			LOG.error("Consul session renew failed", e);
		}
	}

	private void destroyConsulSession() {
		try {
			client.sessionDestroy(consulSessionId, QueryParams.DEFAULT);
		} catch (Exception e) {
			LOG.error("Consul session destroy failed", e);
		}
	}

	public void start() {
		running = true;
		executor.execute(() -> {
			while (running) {
				synchronized (lock) {
					createOrRenewConsulSession();
				}
				try {
					Thread.sleep(1000 * (sessionTtl - 1));
				} catch (InterruptedException ignored) {

				}
			}
		});
	}

	private void afterTaskFinished() {
		if (taskCounter.decrementAndGet() == 0) {
			running = false;
			destroyConsulSession();
		}
	}

	private class TaskDecorator implements Runnable {

		private final ConsulTask task;

		public TaskDecorator(ConsulTask target) {
			this.task = target;
		}

		@Override
		public void run() {
			try {
				task.execute(client, consulSessionId);
			} finally {
				afterTaskFinished();
			}
		}
	}
}
