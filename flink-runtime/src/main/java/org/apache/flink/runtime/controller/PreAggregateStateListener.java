package org.apache.flink.runtime.controller;

import org.fusesource.mqtt.client.*;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static java.nio.charset.StandardCharsets.UTF_8;

public class PreAggregateStateListener extends Thread {

	// This is a Map to store state of each pre-agg physical operator using the subtaskIndex as the key
	public final Map<Integer, PreAggregateState> preAggregateState;

	// Atributes for the MQTT listen channel
	private final String topic;
	private final String host;
	private final int port;
	private BlockingConnection subscriber;
	private MQTT mqtt;
	private boolean running = false;

	public PreAggregateStateListener(String host, int port, String topic) {
		this.host = host;
		this.port = port;
		this.topic = topic;
		this.running = true;
		this.preAggregateState = new HashMap<Integer, PreAggregateState>();
	}

	public void connect() throws Exception {
		this.mqtt = new MQTT();
		this.mqtt.setHost(host, port);
		this.subscriber = mqtt.blockingConnection();
		this.subscriber.connect();
		Topic[] topics = new Topic[]{new Topic(this.topic, QoS.AT_LEAST_ONCE)};
		this.subscriber.subscribe(topics);
	}

	public void cancel() {
		this.running = false;
	}

	public void run() {
		try {
			while (running) {
				// System.out.println("waiting for messages...");
				Message msg = subscriber.receive(10, TimeUnit.SECONDS);
				if (msg != null) {
					msg.ack();
					String message = new String(msg.getPayload(), UTF_8);
					if (message != null) {
						// System.out.println("PreAggregateListener message: " + message);
						this.addState(message);
					} else {
						System.out.println("The parameter sent is null.");
					}
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	private void addState(String msg) {
		String[] states = msg.split("\\|");
		if (states != null && states.length == 18) {
			String subtaskIndex = states[0];
			String outPoolUsageMin = states[1];
			String outPoolUsageMax = states[2];
			String outPoolUsageMean = states[3];
			String outPoolUsage05 = states[4];
			String outPoolUsage075 = states[5];
			String outPoolUsage095 = states[6];
			String outPoolUsage099 = states[7];
			String outPoolUsageStdDev = states[8];
			String latencyMin = states[9];
			String latencyMax = states[10];
			String latencyMean = states[11];
			String latencyQuantile05 = states[12];
			String latencyQuantile099 = states[13];
			String latencyStdDev = states[14];
			String numRecordsInPerSecond = states[15];
			String numRecordsOutPerSecond = states[16];
			String minCountCurrent = states[17];

			PreAggregateState state = this.preAggregateState.get(Integer.parseInt(subtaskIndex));
			if (state == null) {
				state = new PreAggregateState(subtaskIndex, outPoolUsageMin, outPoolUsageMax, outPoolUsageMean,
					outPoolUsage05, outPoolUsage075, outPoolUsage095, outPoolUsage099, outPoolUsageStdDev,
					latencyMin, latencyMax, latencyMean, latencyQuantile05, latencyQuantile099, latencyStdDev,
					numRecordsInPerSecond, numRecordsOutPerSecond, minCountCurrent);
			} else {
				state.update(subtaskIndex, outPoolUsageMin, outPoolUsageMax, outPoolUsageMean,
					outPoolUsage05, outPoolUsage075, outPoolUsage095, outPoolUsage099, outPoolUsageStdDev,
					latencyMin, latencyMax, latencyMean, latencyQuantile05, latencyQuantile099, latencyStdDev,
					numRecordsInPerSecond, numRecordsOutPerSecond, minCountCurrent);
			}
			this.preAggregateState.put(Integer.parseInt(subtaskIndex), state);
		} else {
			System.out.println("ERROR: wrong number of parameter to update pre-aggregate state.");
		}
	}
}
