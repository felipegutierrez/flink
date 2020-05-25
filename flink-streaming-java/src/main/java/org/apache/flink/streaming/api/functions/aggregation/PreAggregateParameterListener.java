package org.apache.flink.streaming.api.functions.aggregation;

import org.apache.flink.shaded.guava18.com.google.common.base.Strings;
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
public class PreAggregateParameterListener extends Thread implements Serializable {

	public static final String TOPIC_PRE_AGGREGATE_PARAMETER = "topic-pre-aggregate-parameter";
	private final String topic;
	private final String host;
	private final int port;
	private final PreAggregateTriggerFunction preAggregateTriggerFunction;
	private final int subtaskIndex;
	private BlockingConnection subscriber;
	private MQTT mqtt;
	private boolean running = false;

	public PreAggregateParameterListener(PreAggregateTriggerFunction preAggregateTriggerFunction, int subtaskIndex) {
		// Job manager and taskManager have to be deployed on the same machine, otherwise use the other constructor
		this(preAggregateTriggerFunction, "127.0.0.1", subtaskIndex);
	}


	public PreAggregateParameterListener(PreAggregateTriggerFunction preAggregateTriggerFunction, String host, int subtaskIndex) {
		this.preAggregateTriggerFunction = preAggregateTriggerFunction;
		if (Strings.isNullOrEmpty(host) || host.equalsIgnoreCase("localhost")) {
			this.host = "127.0.0.1";
		} else {
			this.host = host;
		}
		this.port = 1883;
		this.topic = TOPIC_PRE_AGGREGATE_PARAMETER;
		this.running = true;
		this.subtaskIndex = subtaskIndex;
		this.disclaimer();
	}

	public void connect() throws Exception {
		this.mqtt = new MQTT();
		this.mqtt.setHost(host, port);
		this.subscriber = mqtt.blockingConnection();
		this.subscriber.connect();
		Topic[] topics = new Topic[]{new Topic(this.topic, QoS.AT_LEAST_ONCE)};
		this.subscriber.subscribe(topics);
	}

	public void run() {
		try {
			while (running) {
				// System.out.println("waiting for messages...");
				Message msg = subscriber.receive(10, TimeUnit.SECONDS);
				if (msg != null) {
					msg.ack();
					String message = new String(msg.getPayload(), UTF_8);
					if (isInteger(message)) {
						this.preAggregateTriggerFunction.setMaxCount(Integer.valueOf(message).intValue(), this.subtaskIndex);
					} else {
						System.out.println("The parameter sent is not an integer: " + message);
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
		if (s.isEmpty()) {
			return false;
		}
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
		System.out.println("This is the application [" + PreAggregateParameterListener.class.getSimpleName() + "].");
		System.out.println("It listens new frequency parameters from an MQTT broker.");
		System.out.println("To publish in this broker use:");
		System.out.println("mosquitto_pub -h " + this.host + " -p " + this.port + " -t " + this.topic + " -m \"MaxCount\"");
		System.out.println();
	}
}
