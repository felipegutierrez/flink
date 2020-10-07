package org.apache.flink.streaming.examples.aggregate.util;

import com.codahale.metrics.SlidingTimeWindowArrayReservoir;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.dropwizard.metrics.DropwizardHistogramWrapper;
import org.apache.flink.metrics.Histogram;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.fusesource.hawtbuf.AsciiBuffer;
import org.fusesource.hawtbuf.Buffer;
import org.fusesource.hawtbuf.UTF8Buffer;
import org.fusesource.mqtt.client.Future;
import org.fusesource.mqtt.client.FutureConnection;
import org.fusesource.mqtt.client.MQTT;
import org.fusesource.mqtt.client.QoS;

import java.util.LinkedList;
import java.util.concurrent.TimeUnit;

public class MqttDataSink extends RichSinkFunction<String> {
	private static final String DEFAUL_HOST = "127.0.0.1";
	private static final int DEFAUL_PORT = 1883;
	private final String user = env("ACTIVEMQ_USER", "admin");
	private final String password = env("ACTIVEMQ_PASSWORD", "password");
	private final String topic;
	private final QoS qos;
	private final boolean enableEndToEndLatencyMonitor;
	/**
	 * Histogram and Gauge metrics to monitor latency and network buffer
	 */
	private final String END_TO_END_LATENCY_HISTOGRAM = "end-to-end-latency-histogram";
	private String host = env("ACTIVEMQ_HOST", "localhost");
	private int port = Integer.parseInt(env("ACTIVEMQ_PORT", "1883"));
	private FutureConnection connection;
	private Histogram latencyHistogram;

	public MqttDataSink(String topic) {
		this(DEFAUL_HOST, DEFAUL_PORT, topic, QoS.AT_LEAST_ONCE, false);
	}

	public MqttDataSink(String topic, String host, int port) {
		this(host, port, topic, QoS.AT_LEAST_ONCE, false);
	}

	public MqttDataSink(String topic, String host, int port, boolean enableEndToEndLatencyMonitor) {
		this(host, port, topic, QoS.AT_LEAST_ONCE, enableEndToEndLatencyMonitor);
	}

	public MqttDataSink(String host, String topic) {
		this(host, DEFAUL_PORT, topic, QoS.AT_LEAST_ONCE, false);
	}

	public MqttDataSink(String host, int port, String topic) {
		this(host, port, topic, QoS.AT_LEAST_ONCE, false);
	}

	public MqttDataSink(String host, int port, String topic, QoS qos, boolean enableEndToEndLatencyMonitor) {
		this.host = host;
		this.port = port;
		this.topic = topic;
		this.qos = qos;
		this.enableEndToEndLatencyMonitor = enableEndToEndLatencyMonitor;
		disclaimer();
	}

	private static String env(String key, String defaultValue) {
		String rc = System.getenv(key);
		if (rc == null)
			return defaultValue;
		return rc;
	}

	@Override
	public void open(Configuration config) throws Exception {
		// Open the MQTT connection just once
		MQTT mqtt = new MQTT();
		mqtt.setHost(host, port);
		mqtt.setUserName(user);
		mqtt.setPassword(password);

		this.connection = mqtt.futureConnection();
		this.connection.connect().await();

		// create histogram metrics
		if (this.enableEndToEndLatencyMonitor) {
			int reservoirWindow = 30;
			com.codahale.metrics.Histogram dropwizardLatencyHistogram = new com.codahale.metrics.Histogram(new SlidingTimeWindowArrayReservoir(reservoirWindow, TimeUnit.SECONDS));
			Histogram latencyHistogram = getRuntimeContext().getMetricGroup().histogram(
				END_TO_END_LATENCY_HISTOGRAM, new DropwizardHistogramWrapper(dropwizardLatencyHistogram));
			this.latencyHistogram = latencyHistogram;
		}
	}

	@Override
	public void close() throws Exception {
		super.close();
		connection.disconnect().await();
	}

	@Override
	public void invoke(String value, Context context) throws Exception {
		// System.out.println(PrinterSink.class.getSimpleName() + ": " + value);
		System.out.flush();

		int size = value.length();
		String DATA = value;
		String body = "";
		for (int i = 0; i < size; i++) {
			body += DATA.charAt(i % DATA.length());
		}
		Buffer msg = new AsciiBuffer(body);

		final LinkedList<Future<Void>> queue = new LinkedList<Future<Void>>();
		UTF8Buffer topic = new UTF8Buffer(this.topic);

		// Send the publish without waiting for it to complete. This allows us
		// to send multiple message without blocking..
		queue.add(connection.publish(topic, msg, this.qos, false));
		// Eventually we start waiting for old publish futures to complete
		// so that we don't create a large in memory buffer of outgoing message.s

		// track end-to-end latency
		if (this.enableEndToEndLatencyMonitor) {
			if (msg != null) {
				String[] bodySplit = body.split("-");
				if (bodySplit.length == 3) {
					long ingestionTime = Long.valueOf(bodySplit[2]);
					long end2endLatency = System.currentTimeMillis() - ingestionTime;
					this.latencyHistogram.update(end2endLatency);
					// System.out.println("mean[" + this.latencyHistogram.getStatistics().getMean() + "] 0.99[" + this.latencyHistogram.getStatistics().getQuantile(0.99) + "]");
				}
			}
		}

		if (queue.size() >= 1000) {
			queue.removeFirst().await();
		}

		while (!queue.isEmpty()) {
			queue.removeFirst().await();
		}
	}

	public Histogram getLatencyHistogram() {
		return latencyHistogram;
	}

	private void disclaimer() {
		System.out.println("Consuming data >>>");
		System.out.println("In order to consume data from this application use the command line below:");
		System.out.println("mosquitto_sub -h " + host + " -p " + port + " -t " + topic);
		System.out.println();
	}
}
