package org.apache.flink.streaming.api.functions.aggregation;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.metrics.Histogram;
import org.apache.flink.shaded.guava18.com.google.common.base.Strings;
import org.apache.flink.streaming.util.functions.PreAggLatencyMeanGauge;
import org.apache.flink.streaming.util.functions.PreAggParamGauge;
import org.fusesource.hawtbuf.AsciiBuffer;
import org.fusesource.hawtbuf.Buffer;
import org.fusesource.hawtbuf.UTF8Buffer;
import org.fusesource.mqtt.client.Future;
import org.fusesource.mqtt.client.FutureConnection;
import org.fusesource.mqtt.client.MQTT;
import org.fusesource.mqtt.client.QoS;

import java.io.Serializable;
import java.text.DecimalFormat;
import java.text.SimpleDateFormat;
import java.util.LinkedList;

public class PreAggregateMonitorBkp extends Thread implements Serializable {

	private final SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
	private final DecimalFormat df = new DecimalFormat("#.###");
	private final String TOPIC_PRE_AGG_STATE = "topic-pre-aggregate-state";

	// private final PreAggregateTriggerFunction preAggregateTriggerFunction;
//	private final int subtaskIndex;
//	private final int controllerFrequencySec;
//	private final boolean enableController;
	/**
	 * Histogram and Gauge metrics to monitor latency and network buffer
	 */
//	private final Histogram latencyHistogram;
//	private final Histogram outPoolUsageHistogram;
//	private final PreAggParamGauge preAggParamGauge;
//	private final PreAggLatencyMeanGauge preAggLatencyMeanGauge;
	/**
	 * MQTT broker is used to set the parameter K to all PreAgg operators
	 */
	private MQTT mqtt;
	private FutureConnection connection;
	private String host;
	private int port;

	private boolean running = false;
	private double numRecordsOutPerSecond;
	private double numRecordsInPerSecond;
	private double numRecordsInPerSecondPrev;
	private double currentCapacity;

	private int minCountPrev;

	public PreAggregateMonitorBkp(// PreAggregateTriggerFunction preAggregateTriggerFunction,
								  Histogram latencyHistogram, Histogram outPoolUsageHistogram,
								  PreAggParamGauge preAggParamGauge, PreAggLatencyMeanGauge preAggLatencyMeanGauge,
								  int subtaskIndex, boolean enableController) {
		// this(preAggregateTriggerFunction, latencyHistogram, outPoolUsageHistogram, preAggParamGauge, preAggLatencyMeanGauge, "127.0.0.1", subtaskIndex, enableController);
		// }
	// public PreAggregateMonitorBkp(// PreAggregateTriggerFunction preAggregateTriggerFunction,
								  // Histogram latencyHistogram, Histogram outPoolUsageHistogram,
								  // PreAggParamGauge preAggParamGauge, PreAggLatencyMeanGauge preAggLatencyMeanGauge,
								  // String host, int subtaskIndex, boolean enableController)
								  // {
//		this.enableController = enableController;
//		this.controllerFrequencySec = 60;
		// this.preAggregateTriggerFunction = preAggregateTriggerFunction;
//		this.latencyHistogram = latencyHistogram;
//		this.outPoolUsageHistogram = outPoolUsageHistogram;
//		this.preAggParamGauge = preAggParamGauge;
//		this.preAggLatencyMeanGauge = preAggLatencyMeanGauge;
//		this.subtaskIndex = subtaskIndex;
		this.running = true;
		this.currentCapacity = 0.0;

		try {
			if (Strings.isNullOrEmpty(host) || host.equalsIgnoreCase("localhost")) {
				this.host = "127.0.0.1";
			} else {
				this.host = host;
			}
			this.port = 1883;
		} catch (Exception e) {
			e.printStackTrace();
		}
		this.disclaimer();
	}

	private void disclaimer() {
		// System.out.println("Initialized pre-aggregate monitor for subtask[" + this.subtaskIndex + "] scheduled to every " + this.controllerFrequencySec + " seconds.");
	}

	public void connect() throws Exception {
		mqtt = new MQTT();
		mqtt.setHost(host, port);

		connection = mqtt.futureConnection();
		connection.connect().await();
	}

	private void disconnect() throws Exception {
		connection.disconnect().await();
	}

	public void run() {
		try {
			while (running) {
//				Thread.sleep(controllerFrequencySec * 1000);

				// new estimated PreAgg parameter K
//				Tuple2<Integer, String> newMaxCountPreAggregate = this.computePreAggregateParameter(this.preAggregateTriggerFunction.getMaxCount());

//				if (this.enableController && this.preAggregateTriggerFunction.getPreAggregateStrategy().equals(PreAggregateStrategy.GLOBAL)) {
					// GLOBAL strategy: MQTT global trigger for all operators
//					this.publish(newMaxCountPreAggregate.f1);
//				} else if (this.enableController && this.preAggregateTriggerFunction.getPreAggregateStrategy().equals(PreAggregateStrategy.LOCAL)) {
					// LOCAL strategy: each PreAgg operator receives a different parameter K
//					this.preAggregateTriggerFunction.setMaxCount(newMaxCountPreAggregate.f0.intValue(), this.subtaskIndex);
//				} else if (this.enableController) {
//					System.out.println("ERROR: PreAggregateStrategy [" + this.preAggregateTriggerFunction.getPreAggregateStrategy().getValue() + "] not implemented.");
//				} else {
//					System.out.println("WARNING: PreAgg autonomous controller is not enabled. Manual adjustment is still enable.");
//				}
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

	/**
	 * Computes the new pre-aggregate parameter to achieve minimum emission latency to the pre-aggregate operator.
	 *
	 * @return
	 */
	private Tuple2<Integer, String> computePreAggregateParameter(int minCountCurrent) {
		/*
		long latencyMin = this.latencyHistogram.getStatistics().getMin();
		long latencyMax = this.latencyHistogram.getStatistics().getMax();
		double latencyMean = this.latencyHistogram.getStatistics().getMean();
		double latencyQuantile05 = this.latencyHistogram.getStatistics().getQuantile(0.5);
		double latencyQuantile099 = this.latencyHistogram.getStatistics().getQuantile(0.99);
		double latencyStdDev = this.latencyHistogram.getStatistics().getStdDev();
		long outPoolUsageMin = this.outPoolUsageHistogram.getStatistics().getMin();
		long outPoolUsageMax = this.outPoolUsageHistogram.getStatistics().getMax();
		double outPoolUsageMean = this.outPoolUsageHistogram.getStatistics().getMean();
		double outPoolUsage05 = this.outPoolUsageHistogram.getStatistics().getQuantile(0.5);
		double outPoolUsage075 = this.outPoolUsageHistogram.getStatistics().getQuantile(0.75);
		double outPoolUsage095 = this.outPoolUsageHistogram.getStatistics().getQuantile(0.95);
		double outPoolUsage099 = this.outPoolUsageHistogram.getStatistics().getQuantile(0.99);
		double outPoolUsageStdDev = this.outPoolUsageHistogram.getStatistics().getStdDev();
		int minCountNew = minCountCurrent;
		String label = "=";
		String msg = "";

		if (this.enableController && this.preAggregateTriggerFunction.getPreAggregateStrategy().equals(PreAggregateStrategy.LOCAL)) {
			int percent05 = (int) Math.ceil(minCountCurrent * 0.05);
			int percent30 = (int) Math.ceil(minCountCurrent * 0.3);

			if (minCountNew == this.minCountPrev) {
				// We update the minCount only if it was the same value of the previous one updated by the controller
				// Update the parameter K of the Pre-aggregation operator
				if (outPoolUsageMean >= 27.0 && outPoolUsage099 >= 50.0) {
					// BACKPRESSURE -> increase latency -> increase the pre-aggregation parameter
					if (outPoolUsageMin == 100 && outPoolUsageMax == 100) {
						if (minCountCurrent < 20) {
							minCountNew = minCountCurrent * 2;
							label = "+++";
						} else {
							minCountNew = minCountCurrent + percent30;
							label = "++";
						}
					} else {
						minCountNew = minCountCurrent + percent05;
						label = "+";
					}
					this.currentCapacity = this.numRecordsOutPerSecond;
				} else if (outPoolUsageMean < 27.0 && outPoolUsage075 < 31.0 && outPoolUsage095 < 37.0) {
					// AVAILABLE RESOURCE -> minimize latency -> decrease the pre-aggregation parameter
					if (outPoolUsageMin == 25 && outPoolUsageMax == 25) {
						minCountNew = minCountCurrent - percent30;
						label = "--";
					} else {
						// if the output throughput is greater than the capacity we don't decrease the parameter K
						if (this.currentCapacity == 0 || this.numRecordsOutPerSecond < this.currentCapacity) {
							minCountNew = minCountCurrent - percent05;
							label = "-";
						}
					}
				}
			}
		}

		if (this.preAggregateTriggerFunction.getPreAggregateStrategy().equals(PreAggregateStrategy.GLOBAL)) {
			msg = this.subtaskIndex + "|" +
				outPoolUsageMin + "|" + outPoolUsageMax + "|" + outPoolUsageMean + "|" + outPoolUsage05 + "|" +
				outPoolUsage075 + "|" + outPoolUsage095 + "|" + outPoolUsage099 + "|" + outPoolUsageStdDev + "|" +
				latencyMin + "|" + latencyMax + "|" + latencyMean + "|" + latencyQuantile05 + "|" + latencyQuantile099 + "|" +
				latencyStdDev + "|" + this.numRecordsInPerSecond + "|" + this.numRecordsOutPerSecond + "|" + minCountCurrent;
			// Update parameters to Prometheus+Grafana
			this.preAggParamGauge.updateValue(minCountCurrent);
		} else if (this.preAggregateTriggerFunction.getPreAggregateStrategy().equals(PreAggregateStrategy.LOCAL)) {
			msg = "Controller[" + this.subtaskIndex + "]" +
				" OutPoolUsg min[" + outPoolUsageMin + "]max[" + outPoolUsageMax + "]mean[" + outPoolUsageMean +
				"]0.5[" + outPoolUsage05 + "]0.75[" + outPoolUsage075 + "]0.95[" + outPoolUsage095 + "]0.99[" + outPoolUsage099 + "]StdDev[" + df.format(outPoolUsageStdDev) + "]" +
				" Latency min[" + latencyMin + "]max[" + latencyMax + "]mean[" + latencyMean +
				"]0.5[" + latencyQuantile05 + "]0.99[" + latencyQuantile099 + "]StdDev[" + df.format(latencyStdDev) + "]" +
				" IN_prev[" + df.format(this.numRecordsInPerSecondPrev) + "]IN[" + df.format(this.numRecordsInPerSecond) +
				"] OUT[" + df.format(this.numRecordsOutPerSecond) + "]threshold[" + df.format(this.currentCapacity) + "]" +
				" K" + label + ": " + minCountCurrent + "->" + minCountNew;
			System.out.println(msg);
			// Update parameters to Prometheus+Grafana
			this.preAggParamGauge.updateValue(minCountNew);
		}

		// Update the previous input throughput of the operator
		this.numRecordsInPerSecondPrev = this.numRecordsInPerSecond;

		// Update parameters to Prometheus+Grafana
		this.preAggLatencyMeanGauge.updateValue(latencyMean);
		this.minCountPrev = minCountNew;

		return Tuple2.of(minCountNew, msg);
		 */
		return null;
	}

	public void cancel() {
		this.running = false;
	}

	public Histogram getLatencyHistogram() {
		return null; // return latencyHistogram;
	}

	public Histogram getOutPoolUsageHistogram() {
		return null; // return outPoolUsageHistogram;
	}

	public void setNumRecordsOutPerSecond(double numRecordsOutPerSecond) {
		this.numRecordsOutPerSecond = numRecordsOutPerSecond;
	}

	public void setNumRecordsInPerSecond(double numRecordsInPerSecond) {
		this.numRecordsInPerSecond = numRecordsInPerSecond;
	}
}
