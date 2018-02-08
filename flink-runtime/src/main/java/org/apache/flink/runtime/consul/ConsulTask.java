package org.apache.flink.runtime.consul;

import com.ecwid.consul.v1.ConsulClient;

public interface ConsulTask {

	void execute(ConsulClient client, String sessionId);
}
