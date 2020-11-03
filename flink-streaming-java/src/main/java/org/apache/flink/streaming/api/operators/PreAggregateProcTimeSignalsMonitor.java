package org.apache.flink.streaming.api.operators;

import org.apache.flink.metrics.Histogram;
import org.apache.flink.streaming.util.functions.PreAggIntervalMsGauge;

import org.apache.flink.shaded.guava18.com.google.common.base.Strings;

import org.fusesource.hawtbuf.AsciiBuffer;
import org.fusesource.hawtbuf.Buffer;
import org.fusesource.hawtbuf.UTF8Buffer;
import org.fusesource.mqtt.client.Future;
import org.fusesource.mqtt.client.FutureConnection;
import org.fusesource.mqtt.client.MQTT;
import org.fusesource.mqtt.client.QoS;

import java.io.Serializable;
import java.util.LinkedList;

/**
 * This class listen to signals on the pre-agg operators and send them to the controller in the JobManager.
 */
public class PreAggregateProcTimeSignalsMonitor extends Thread implements Serializable {

	/** topi on the JobManager controller */
	private final String TOPIC_PRE_AGG_STATE = "topic-pre-aggregate-state";
	/** Histogram metrics to monitor network buffer */
	private final Histogram outPoolUsageHistogram;
	/** Gauge metrics to monitor latency parameter */
	private final PreAggIntervalMsGauge preAggIntervalMsGauge;
	private final int controllerFrequencySec;
	private final int subtaskId;
	private final boolean enableController;
	private final String host;
	private final int port;
	private long intervalMs;
	/** throughput of the operator */
	private double numRecordsOutPerSecond;
	private double numRecordsInPerSecond;
	/** MQTT broker is used to send signals of each pre-agg operator to the JobManager controller */
	private MQTT mqtt;
	private FutureConnection connection;
	private boolean running = false;

	public PreAggregateProcTimeSignalsMonitor(
		Histogram outPoolUsageHistogram,
		PreAggIntervalMsGauge preAggIntervalMsGauge,
		String jobManagerAddress,
		int subtaskId,
		boolean enableController) {

		this.outPoolUsageHistogram = outPoolUsageHistogram;
		this.preAggIntervalMsGauge = preAggIntervalMsGauge;
		this.running = true;
		this.controllerFrequencySec = 60;
		this.subtaskId = subtaskId;
		this.enableController = enableController;

		this.host = (Strings.isNullOrEmpty(jobManagerAddress) || jobManagerAddress.equalsIgnoreCase(
			"localhost")) ? "127.0.0.1" : jobManagerAddress;
		this.port = 1883;
	}

	private void connect() throws Exception {
		mqtt = new MQTT();
		mqtt.setHost(host, port);

		connection = mqtt.futureConnection();
		connection.connect().await();
	}

	private void disconnect() throws Exception {
		connection.disconnect().await();
	}

	@Override
	public void run() {
		try {
			if (mqtt == null) this.connect();
			while (running) {
				Thread.sleep(controllerFrequencySec * 1000);
				String preAggSignals = this.collectSignals();
				this.publish(preAggSignals);
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	private void publish(String message) throws Exception {
		final LinkedList<Future<Void>> queue = new LinkedList<Future<Void>>();
		UTF8Buffer topic = new UTF8Buffer(TOPIC_PRE_AGG_STATE);
		Buffer msg = new AsciiBuffer(message);

		// Send the publish without waiting for it to complete. This allows us to send multiple message without blocking.
		queue.add(connection.publish(topic, msg, QoS.AT_LEAST_ONCE, false));

		while (!queue.isEmpty()) {
			queue.removeFirst().await();
		}
	}

	private String collectSignals() {
		long outPoolUsageMin = this.outPoolUsageHistogram.getStatistics().getMin();
		long outPoolUsageMax = this.outPoolUsageHistogram.getStatistics().getMax();
		double outPoolUsageMean = this.outPoolUsageHistogram.getStatistics().getMean();
		double outPoolUsage05 = this.outPoolUsageHistogram.getStatistics().getQuantile(0.5);
		double outPoolUsage075 = this.outPoolUsageHistogram.getStatistics().getQuantile(0.75);
		double outPoolUsage095 = this.outPoolUsageHistogram.getStatistics().getQuantile(0.95);
		double outPoolUsage099 = this.outPoolUsageHistogram.getStatistics().getQuantile(0.99);
		double outPoolUsageStdDev = this.outPoolUsageHistogram.getStatistics().getStdDev();

		String msg =
			this.subtaskId + "|" + outPoolUsageMin + "|" + outPoolUsageMax + "|" + outPoolUsageMean
				+ "|" + outPoolUsage05 + "|" + outPoolUsage075 + "|" + outPoolUsage095 + "|"
				+ outPoolUsage099 + "|" + outPoolUsageStdDev + "|" + this.numRecordsInPerSecond
				+ "|" + this.numRecordsOutPerSecond + "|" + this.intervalMs;

		// Update parameters to Prometheus+Grafana
		this.preAggIntervalMsGauge.updateValue(this.intervalMs);

		return msg;
	}

	public void setIntervalMs(long intervalMs) {
		this.intervalMs = intervalMs;
	}

	public void cancel() {
		this.running = false;
	}
}
