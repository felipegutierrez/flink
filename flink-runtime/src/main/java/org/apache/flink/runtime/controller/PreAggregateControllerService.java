package org.apache.flink.runtime.controller;

import org.apache.flink.shaded.guava18.com.google.common.base.Strings;
import org.fusesource.hawtbuf.AsciiBuffer;
import org.fusesource.hawtbuf.Buffer;
import org.fusesource.hawtbuf.UTF8Buffer;
import org.fusesource.mqtt.client.Future;
import org.fusesource.mqtt.client.FutureConnection;
import org.fusesource.mqtt.client.MQTT;
import org.fusesource.mqtt.client.QoS;

import java.text.DecimalFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.LinkedList;
import java.util.Map;

/**
 * The PreAggregate controller listens to all preAggregation operators metrics and publish a global pre-aggregate parameter
 * K on the preAggregation operators.
 */
public class PreAggregateControllerService extends Thread {

	private final SimpleDateFormat sdf = new SimpleDateFormat("HH:mm:ss.SSS");
	private final DecimalFormat df = new DecimalFormat("#.###");
	private final String TOPIC_PRE_AGG_PARAMETER = "topic-pre-aggregate-parameter";
	private final String TOPIC_PRE_AGG_STATE = "topic-pre-aggregate-state";
	private final int controllerFrequencySec;
	private final boolean running;
	private final PreAggregateStateListener preAggregateListener;
	/**
	 * MQTT broker is used to set the parameter K to all PreAgg operators
	 */
	private MQTT mqtt;
	private FutureConnection connection;
	private String host;
	private int port;

	// global states
	private double numRecordsInPerSecondMax;
	private double numRecordsOutPerSecondMax;

	public PreAggregateControllerService() throws Exception {
		// Job manager and taskManager have to be deployed on the same machine, otherwise use the other constructor
		this("127.0.0.1");
	}

	public PreAggregateControllerService(String brokerServerHost) throws Exception {
		this.numRecordsOutPerSecondMax = 0.0;
		this.controllerFrequencySec = 120;
		this.running = true;
		try {
			if (Strings.isNullOrEmpty(brokerServerHost)) {
				this.host = "127.0.0.1";
			} else {
				this.host = brokerServerHost;
			}
			this.port = 1883;
		} catch (Exception e) {
			e.printStackTrace();
		}
		this.preAggregateListener = new PreAggregateStateListener(this.host, this.port, TOPIC_PRE_AGG_STATE);
		this.preAggregateListener.connect();
		this.preAggregateListener.start();
		this.disclaimer();
	}

	private void disclaimer() {
		System.out.println("Initialized pre-aggregate Controller service scheduled to every " + this.controllerFrequencySec + " seconds.");
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
		while (running) {
			try {
				Thread.sleep(this.controllerFrequencySec * 1000);
				publish(computePreAggregateParameter());
			} catch (InterruptedException e) {
				e.printStackTrace();
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
	}

	/**
	 * Compute the new global pre-aggregate parameter based on the state stored at this.preAggregateListener.preAggregateState.
	 *
	 * @return
	 */
	private int computePreAggregateParameter() {
		Integer preAggregateParameter = 0;
		MinCount minCount = new MinCount();

		for (Map.Entry<Integer, PreAggregateState> entry : this.preAggregateListener.preAggregateState.entrySet()) {
			String label = "";
			Integer subtaskIndex = entry.getKey();
			PreAggregateState preAggregateState = entry.getValue();

			int minCountPercent05 = (int) Math.ceil(preAggregateState.getMinCount() * 0.05);
			int minCountPercent20 = (int) Math.ceil(preAggregateState.getMinCount() * 0.2);
			int minCountPercent40 = (int) Math.ceil(preAggregateState.getMinCount() * 0.4);

			if (preAggregateState.getOutPoolUsageMean() > 33.0 && preAggregateState.getOutPoolUsage099() >= 50.0) {
				// BACKPRESSURE -> increase latency -> increase the pre-aggregation parameter
				if (preAggregateState.getOutPoolUsageMin() == 100 && preAggregateState.getOutPoolUsageMax() == 100) {
					if (preAggregateState.getMinCount() <= 20) {
						minCount.update(preAggregateState.getMinCount() * 2);
						label = "+++";
					} else {
						// If it is the second time that we see a physical operator overloaded we increase the latency by 40%
						if (!minCount.isOverloaded()) {
							minCount.update(preAggregateState.getMinCount() + minCountPercent20);
						} else {
							minCount.update(preAggregateState.getMinCount() + minCountPercent40);
						}
						label = "++";
					}
					// If at least one physical operator is overloaded (100%) we consider to increase latency
					// no matter the state of the any other physical operator
					minCount.setOverloaded(true);
				} else {
					minCount.update(preAggregateState.getMinCount() + minCountPercent05);
					label = "+";
					if (preAggregateState.getNumRecordsInPerSecond() >= (this.numRecordsInPerSecondMax * 0.95)) {
						// If the input throughput is close to the max input throughput in 95% invalidate the increase latency action
						minCount.setValidate(false);
					}
				}
				this.updateGlobalCapacity(preAggregateState.getNumRecordsInPerSecond(), preAggregateState.getNumRecordsOutPerSecond());
			} else if (preAggregateState.getOutPoolUsageMean() <= 33.0 && preAggregateState.getOutPoolUsage075() <= 33.0 &&
				preAggregateState.getOutPoolUsage095() < 50.0) {
				// AVAILABLE RESOURCE -> minimize latency -> decrease the pre-aggregation parameter
				if (preAggregateState.getOutPoolUsageMin() == 25 && preAggregateState.getOutPoolUsageMax() == 25) {
					minCount.update(preAggregateState.getMinCount() - minCountPercent20);
					label = "--";
				} else {
					// if the output throughput is greater than the capacity we don't decrease the parameter K
					if (this.numRecordsOutPerSecondMax == 0 || preAggregateState.getNumRecordsOutPerSecond() < this.numRecordsOutPerSecondMax) {
						minCount.update(preAggregateState.getMinCount() - minCountPercent05);
						label = "-";
						if (preAggregateState.getNumRecordsInPerSecond() >= (this.numRecordsInPerSecondMax * 0.95)) {
							// If the input throughput is close to the max input throughput in 95% invalidate the decrease latency action
							minCount.setValidate(false);
						}
					}
				}
			} else {
				if (preAggregateState.getNumRecordsInPerSecond() >= (this.numRecordsInPerSecondMax * 0.95)) {
					// this is the same lock of increasing and decreasing latency
					minCount.setValidate(false);
				}
			}
			String msg = "Controller-" + subtaskIndex +
				" outPool-min[" + preAggregateState.getOutPoolUsageMin() + "]max[" + preAggregateState.getOutPoolUsageMax() +
				"]mean[" + preAggregateState.getOutPoolUsageMean() + "]50[" + preAggregateState.getOutPoolUsage05() +
				"]75[" + preAggregateState.getOutPoolUsage075() + "]95[" + preAggregateState.getOutPoolUsage095() +
				"]99[" + preAggregateState.getOutPoolUsage099() + "]StdDev[" + df.format(preAggregateState.getOutPoolUsageStdDev()) + "]" +
				// " Latency-min[" + preAggregateState.getLatencyMin() + "]max[" + preAggregateState.getLatencyMax() +
				// "]mean[" + preAggregateState.getLatencyMean() + "]50[" + preAggregateState.getLatencyQuantile05() +
				// "]99[" + preAggregateState.getLatencyQuantile099() + "]StdDev[" + df.format(preAggregateState.getLatencyStdDev()) + "]" +
				" IN[" + df.format(preAggregateState.getNumRecordsInPerSecond()) + "]max[" + df.format(this.numRecordsInPerSecondMax) + "]" +
				" OUT[" + df.format(preAggregateState.getNumRecordsOutPerSecond()) + "]max[" + df.format(this.numRecordsOutPerSecondMax) + "]" +
				" K" + label + ": " + preAggregateState.getMinCount() + "-[" + minCount.isValidate() + "]" +
				" t:" + sdf.format(new Date(System.currentTimeMillis()));
			System.out.println(msg);
		}
		if (minCount.isOverloaded() || minCount.isValidate()) {
			preAggregateParameter = minCount.getAverage();
		}
		System.out.println("Next global preAgg parameter K: " + preAggregateParameter);
		return preAggregateParameter;
	}

	private void updateGlobalCapacity(double numRecordsInPerSecond, double numRecordsOutPerSecond) {
		if (numRecordsInPerSecond > this.numRecordsInPerSecondMax) {
			this.numRecordsInPerSecondMax = numRecordsInPerSecond;
		}
		if (numRecordsOutPerSecond > this.numRecordsOutPerSecondMax) {
			this.numRecordsOutPerSecondMax = numRecordsOutPerSecond;
		}
	}

	/**
	 * This method is used to publish the new global pre-aggregation parameter K
	 *
	 * @param newMaxCountPreAggregate
	 * @throws Exception
	 */
	private void publish(int newMaxCountPreAggregate) throws Exception {
		final LinkedList<Future<Void>> queue = new LinkedList<Future<Void>>();
		UTF8Buffer topic = new UTF8Buffer(TOPIC_PRE_AGG_PARAMETER);
		Buffer msg = new AsciiBuffer(Integer.toString(newMaxCountPreAggregate));

		// Send the publish without waiting for it to complete. This allows us to send multiple message without blocking.
		queue.add(connection.publish(topic, msg, QoS.AT_LEAST_ONCE, false));

		while (!queue.isEmpty()) {
			queue.removeFirst().await();
		}
	}

	private class MinCount {
		private Integer minimum;
		private Integer maximum;
		private boolean validate;
		private boolean overloaded;

		public MinCount() {
			this.validate = true;
			this.overloaded = false;
		}

		public void update(int minCount) {
			if (this.minimum == null && this.maximum == null) {
				this.minimum = Integer.valueOf(minCount);
				this.maximum = Integer.valueOf(minCount);
			} else {
				if (this.minimum != null && minCount < this.minimum.intValue()) {
					this.minimum = Integer.valueOf(minCount);
				}
				if (this.maximum != null && minCount > this.maximum.intValue()) {
					this.maximum = Integer.valueOf(minCount);
				}
			}
		}

		public int getAverage() {
			if (this.minimum == null || this.maximum == null) {
				return 0;
			}
			return (this.minimum.intValue() + this.maximum.intValue()) / 2;
		}

		public boolean isValidate() {
			return validate;
		}

		public void setValidate(boolean validate) {
			this.validate = validate;
		}

		public boolean isOverloaded() {
			return this.overloaded;
		}

		public void setOverloaded(boolean overloaded) {
			this.overloaded = overloaded;
		}
	}
}
