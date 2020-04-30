package org.apache.flink.runtime.controller;

import org.fusesource.hawtbuf.AsciiBuffer;
import org.fusesource.hawtbuf.Buffer;
import org.fusesource.hawtbuf.UTF8Buffer;
import org.fusesource.mqtt.client.Future;
import org.fusesource.mqtt.client.FutureConnection;
import org.fusesource.mqtt.client.MQTT;
import org.fusesource.mqtt.client.QoS;

import java.text.DecimalFormat;
import java.text.SimpleDateFormat;
import java.util.LinkedList;
import java.util.Map;

/**
 * The PreAggregate controller listens to all preAggregation operators metrics and publish a global parameter K on the preAggregation operators.
 */
public class PreAggregateControllerService extends Thread {

	private final SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
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
		this.numRecordsOutPerSecondMax = 0.0;
		this.controllerFrequencySec = 60;
		this.running = true;
		try {
			// Job manager and taskManager have to be deployed on the same machine
			this.host = "127.0.0.1";
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
				publish(computePreAggregateParameter());
				Thread.sleep(this.controllerFrequencySec * 1000);
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
		int countSubTasks = 0;
		int minCountSum = 0;
		for (Map.Entry<Integer, PreAggregateState> entry : this.preAggregateListener.preAggregateState.entrySet()) {
			String label = "";
			Integer subtaskIndex = entry.getKey();
			PreAggregateState preAggregateState = entry.getValue();

			int minCountPercent05 = (int) Math.ceil(preAggregateState.getMinCount() * 0.05);
			int minCountPercent30 = (int) Math.ceil(preAggregateState.getMinCount() * 0.3);

			if (preAggregateState.getOutPoolUsageMean() > 33.0 && preAggregateState.getOutPoolUsage099() >= 50.0) {
				// BACKPRESSURE -> increase latency -> increase the pre-aggregation parameter
				if (preAggregateState.getOutPoolUsageMin() == 100 && preAggregateState.getOutPoolUsageMax() == 100) {
					if (preAggregateState.getMinCount() <= 20) {
						minCountSum = minCountSum + preAggregateState.getMinCount() * 2;
						countSubTasks++;
						label = "+++";
					} else {
						minCountSum = minCountSum + (preAggregateState.getMinCount() + minCountPercent30);
						countSubTasks++;
						label = "++";
					}
				} else {
					minCountSum = minCountSum + (preAggregateState.getMinCount() + minCountPercent05);
					countSubTasks++;
					label = "+";
				}
				this.updateGlobalCapacity(preAggregateState.getNumRecordsInPerSecond(), preAggregateState.getNumRecordsOutPerSecond());
			} else if (preAggregateState.getOutPoolUsageMean() <= 27.0 && preAggregateState.getOutPoolUsage075() < 31.0 &&
				preAggregateState.getOutPoolUsage095() < 37.0) {
				// AVAILABLE RESOURCE -> minimize latency -> decrease the pre-aggregation parameter
				if (preAggregateState.getOutPoolUsageMin() == 25 && preAggregateState.getOutPoolUsageMax() == 25) {
					minCountSum = minCountSum + (preAggregateState.getMinCount() - minCountPercent30);
					countSubTasks++;
					label = "--";
				} else {
					// if the output throughput is greater than the capacity we don't decrease the parameter K
					if (this.numRecordsOutPerSecondMax == 0 || preAggregateState.getNumRecordsOutPerSecond() < this.numRecordsOutPerSecondMax) {
						minCountSum = minCountSum + (preAggregateState.getMinCount() - minCountPercent05);
						countSubTasks++;
						label = "-";
					}
				}
			}

			String msg = "Controller[" + subtaskIndex + "]" +
				" OutPoolUsg min[" + preAggregateState.getOutPoolUsageMin() + "]max[" + preAggregateState.getOutPoolUsageMax() +
				"]mean[" + preAggregateState.getOutPoolUsageMean() + "]0.5[" + preAggregateState.getOutPoolUsage05() +
				"]0.75[" + preAggregateState.getOutPoolUsage075() + "]0.95[" + preAggregateState.getOutPoolUsage095() +
				"]0.99[" + preAggregateState.getOutPoolUsage099() + "]StdDev[" + df.format(preAggregateState.getOutPoolUsageStdDev()) + "]" +
				" Latency min[" + preAggregateState.getLatencyMin() + "]max[" + preAggregateState.getLatencyMax() +
				"]mean[" + preAggregateState.getLatencyMean() + "]0.5[" + preAggregateState.getLatencyQuantile05() +
				"]0.99[" + preAggregateState.getLatencyQuantile099() + "]StdDev[" + df.format(preAggregateState.getLatencyStdDev()) + "]" +
				" IN[" + df.format(preAggregateState.getNumRecordsInPerSecond()) + "]max[" + df.format(this.numRecordsInPerSecondMax) + "]" +
				" OUT[" + df.format(preAggregateState.getNumRecordsOutPerSecond()) + "]max[" + df.format(this.numRecordsOutPerSecondMax) + "]" +
				" K" + label + ": " + preAggregateState.getMinCount();
			System.out.println(msg);
		}
		if (countSubTasks > 0) {
			preAggregateParameter = (int) Math.ceil(minCountSum / countSubTasks);
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
}
