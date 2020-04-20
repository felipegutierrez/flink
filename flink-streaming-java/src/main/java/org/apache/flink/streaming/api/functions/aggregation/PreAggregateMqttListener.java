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

	private final String topic;
	private final String host;
	private final int port;
	private final PreAggregateTriggerFunction preAggregateTriggerFunction;
	private final int subtaskIndex;
	private BlockingConnection subscriber;
	private MQTT mqtt;
	private boolean running = false;


	public PreAggregateMqttListener(PreAggregateTriggerFunction preAggregateTriggerFunction, int subtaskIndex) {
		this(preAggregateTriggerFunction, "127.0.0.1", 1883, subtaskIndex);
	}

	public PreAggregateMqttListener(PreAggregateTriggerFunction preAggregateTriggerFunction, String host, int port, int subtaskIndex) {
		this.preAggregateTriggerFunction = preAggregateTriggerFunction;
		this.host = host;
		this.port = port;
		this.running = true;
		this.topic = PreAggregateTriggerFunction.TOPIC_PRE_AGGREGATE_PARAMETER;
		this.subtaskIndex = subtaskIndex;
		// if (this.subtaskIndex == -1) {
		// 	this.topic = PreAggregateTriggerFunction.TOPIC_PRE_AGGREGATE_PARAMETER;
		// } else {
		// 	this.topic = PreAggregateTriggerFunction.TOPIC_PRE_AGGREGATE_PARAMETER + "-" + this.subtaskIndex;
		// }
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
						this.preAggregateTriggerFunction.setMaxCount(Integer.valueOf(message).intValue(), this.subtaskIndex, this.preAggregateTriggerFunction.getPreAggregateStrategy());
						// this.preAggregateTriggerFunction.setMaxCount(Integer.valueOf(message).intValue());
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
		System.out.println("This is the application [" + PreAggregateMqttListener.class.getSimpleName() + "].");
		System.out.println("It listens new frequency parameters from an MQTT broker.");
		System.out.println("To publish in this broker use:");
		System.out.println("mosquitto_pub -h " + this.host + " -p " + this.port + " -t " + this.topic + " -m \"MaxCount\"");
		System.out.println();
	}
}
