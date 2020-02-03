package org.apache.flink.streaming.api.functions.aggregation;

import org.fusesource.mqtt.client.*;

import java.io.Serializable;
import java.util.concurrent.TimeUnit;

import static java.nio.charset.StandardCharsets.UTF_8;

/**
 * <pre>
 *      Changes the frequency that the pre-aggregate emits batches of data:
 * mosquitto_pub -h 127.0.0.1 -p 1883 -t topic-frequency-pre-aggregate -m "1000"
 * </pre>
 */
public class PreAggregateMqttListener extends Thread implements Serializable {

	private static final String TOPIC_REDUCER_OUT_POOL_USAGE = "topic-frequency-pre-aggregate";
	private BlockingConnection subscriber;
	private MQTT mqtt;
	private String host;
	private int port;
	private boolean running = false;
	private PreAggregateTriggerFunction preAggregateTriggerFunction;

	public PreAggregateMqttListener(PreAggregateTriggerFunction preAggregateTriggerFunction) {
		this(preAggregateTriggerFunction, "127.0.0.1", 1883);
	}

	public PreAggregateMqttListener(PreAggregateTriggerFunction preAggregateTriggerFunction, String host, int port) {
		this.preAggregateTriggerFunction = preAggregateTriggerFunction;
		this.host = host;
		this.port = port;
		this.running = true;
		this.disclaimer();
	}

	public void connect() throws Exception {
		mqtt = new MQTT();
		mqtt.setHost(host, port);

		subscriber = mqtt.blockingConnection();
		subscriber.connect();
		Topic[] topics = new Topic[]{new Topic(TOPIC_REDUCER_OUT_POOL_USAGE, QoS.AT_MOST_ONCE)};
		subscriber.subscribe(topics);
	}

	public void run() {
		try {
			while (running) {
				// System.out.println("waiting for messages...");
				Message msg = subscriber.receive(10, TimeUnit.SECONDS);
				if (msg != null) {
					msg.ack();
					String message = new String(msg.getPayload(), UTF_8);
					System.out.println(message);
					if (isInteger(message)) {
						this.preAggregateTriggerFunction.setMaxCount(Long.valueOf(message).longValue());
					}
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public void cancel() {
		this.running = false;
	}

	private boolean isInteger(String s) {
		return isInteger(s, 10);
	}

	private boolean isInteger(String s, int radix) {
		if (s.isEmpty())
			return false;
		for (int i = 0; i < s.length(); i++) {
			if (i == 0 && s.charAt(i) == '-') {
				if (s.length() == 1)
					return false;
				else
					continue;
			}
			if (Character.digit(s.charAt(i), radix) < 0)
				return false;
		}
		return true;
	}

	private void disclaimer() {
		System.out.println("This is the application [" + PreAggregateMqttListener.class.getSimpleName() + "].");
		System.out.println("It listens new frequency parameters from an MQTT broker.");
		System.out.println("To publish in this broker use:");
		System.out.println("mosquitto_pub -h " + host + " -p " + port + " -t " + TOPIC_REDUCER_OUT_POOL_USAGE + " -m \"MaxCount\"");
		System.out.println();
	}
}
