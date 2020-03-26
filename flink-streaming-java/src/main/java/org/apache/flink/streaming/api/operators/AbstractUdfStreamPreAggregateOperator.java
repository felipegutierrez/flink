package org.apache.flink.streaming.api.operators;

import com.codahale.metrics.SlidingWindowReservoir;
import org.apache.flink.api.common.functions.PreAggregateFunction;
import org.apache.flink.dropwizard.metrics.DropwizardHistogramWrapper;
import org.apache.flink.metrics.Gauge;
import org.apache.flink.metrics.Histogram;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.runtime.metrics.groups.OperatorMetricGroup;
import org.apache.flink.runtime.metrics.groups.TaskMetricGroup;
import org.apache.flink.streaming.api.functions.aggregation.*;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;

import java.util.HashMap;
import java.util.Map;

import static org.apache.flink.util.Preconditions.checkNotNull;

public abstract class AbstractUdfStreamPreAggregateOperator<K, V, IN, OUT>
	extends AbstractUdfStreamOperator<OUT, PreAggregateFunction<K, V, IN, OUT>>
	implements OneInputStreamOperator<IN, OUT>, PreAggregateTriggerCallback {

	private static final long serialVersionUID = 1L;
	private final String PRE_AGGREGATE_LATENCY_HISTOGRAM = "pre-aggregate-latency-histogram";
	private final String PRE_AGGREGATE_OUT_POOL_USAGE_HISTOGRAM = "pre-aggregate-outPoolUsage-histogram";

	/**
	 * The map in heap to store elements.
	 */
	private final Map<K, V> bundle;

	/**
	 * The trigger that determines how many elements should be put into a bundle.
	 */
	private final PreAggregateTriggerFunction<IN> preAggregateTrigger;

	/**
	 * Output for stream records.
	 */
	private transient TimestampedCollector<OUT> collector;
	private transient int numOfElements;

	/**
	 * A Mqtt topic to change the parameter K of pre-aggregating items
	 */
	private PreAggregateMqttListener preAggregateMqttListener;

	/**
	 * A Feedback loop PI Controller to find the optimal parameter K of pre-aggregating items
	 */
	private PreAggregateController preAggregateController;
	private boolean controller;

	/**
	 * Histogram metrics to monitor latency and network buffer
	 */
	// private Histogram latencyHistogram;
	// private Histogram outPoolUsageHistogram;
	private long elapsedTime;

	/**
	 * The ID of this subTask
	 */
	private int subtaskIndex;

	/**
	 * @param function
	 * @param preAggregateTrigger
	 */
	public AbstractUdfStreamPreAggregateOperator(PreAggregateFunction<K, V, IN, OUT> function,
												 PreAggregateTriggerFunction<IN> preAggregateTrigger,
												 boolean controller) {
		super(function);
		this.chainingStrategy = ChainingStrategy.ALWAYS;
		this.controller = controller;
		this.bundle = new HashMap<>();
		this.preAggregateTrigger = checkNotNull(preAggregateTrigger, "bundleTrigger is null");
	}

	@Override
	public void open() throws Exception {
		super.open();

		this.numOfElements = 0;
		this.collector = new TimestampedCollector<>(output);

		this.preAggregateTrigger.registerCallback(this);
		// reset trigger
		this.preAggregateTrigger.reset();

		this.subtaskIndex = getRuntimeContext().getIndexOfThisSubtask();
		this.elapsedTime = System.currentTimeMillis();

		// create histogram metrics
		com.codahale.metrics.Histogram dropwizardLatencyHistogram = new com.codahale.metrics.Histogram(new SlidingWindowReservoir(10));
		Histogram latencyHistogram = getRuntimeContext().getMetricGroup().histogram(
			PRE_AGGREGATE_LATENCY_HISTOGRAM, new DropwizardHistogramWrapper(dropwizardLatencyHistogram));
		com.codahale.metrics.Histogram dropwizardOutPoolBufferHistogram = new com.codahale.metrics.Histogram(new SlidingWindowReservoir(10));
		Histogram outPoolUsageHistogram = getRuntimeContext().getMetricGroup().histogram(
			PRE_AGGREGATE_OUT_POOL_USAGE_HISTOGRAM, new DropwizardHistogramWrapper(dropwizardOutPoolBufferHistogram));

		// initiate the PI Controller with the histogram metrics
		this.preAggregateController = new PreAggregateController(this.preAggregateTrigger,
			latencyHistogram, outPoolUsageHistogram, this.subtaskIndex);

		try {
			if (this.preAggregateTrigger.getPreAggregateStrategy() == PreAggregateStrategy.GLOBAL) {
				this.preAggregateMqttListener = new PreAggregateMqttListener(this.preAggregateTrigger);
			} else if (this.preAggregateTrigger.getPreAggregateStrategy() == PreAggregateStrategy.LOCAL) {
				this.preAggregateMqttListener = new PreAggregateMqttListener(this.preAggregateTrigger, this.subtaskIndex);
			} else if (this.preAggregateTrigger.getPreAggregateStrategy() == PreAggregateStrategy.PER_KEY) {
				System.out.println("Pre-aggregate per-key strategy not implemented.");
			} else {
				System.out.println("Pre-aggregate strategy not implemented.");
			}
			this.preAggregateMqttListener.connect();
			this.preAggregateMqttListener.start();
		} catch (Exception e) {
			e.printStackTrace();
		}

		// initialize the PI controller for the pre-aggregate operator
		if (this.controller) {
			this.preAggregateController.start();
		}
	}

	@Override
	public void processElement(StreamRecord<IN> element) throws Exception {
		// get the key and value for the map bundle
		final IN input = element.getValue();
		final K bundleKey = getKey(input);
		final V bundleValue = this.bundle.get(bundleKey);

		// get a new value after adding this element to bundle
		final V newBundleValue = this.userFunction.addInput(bundleValue, input);

		// update to map bundle
		this.bundle.put(bundleKey, newBundleValue);

		this.numOfElements++;
		this.preAggregateTrigger.onElement(input);
	}

	protected abstract K getKey(final IN input) throws Exception;

	@Override
	public void collect() throws Exception {
		if (!this.bundle.isEmpty()) {
			this.numOfElements = 0;
			this.userFunction.collect(bundle, collector);
			this.bundle.clear();
		}
		this.preAggregateTrigger.reset();

		// update metrics to Prometheus+Grafana and reset latency elapsed
		long latency = System.currentTimeMillis() - elapsedTime;
		this.elapsedTime = System.currentTimeMillis();
		this.preAggregateController.getLatencyHistogram().update(latency);

		// update outPoolUsage size metrics to Prometheus+Grafana
		float outPoolUsage = 0.0f;
		OperatorMetricGroup operatorMetricGroup = (OperatorMetricGroup) this.getMetricGroup();
		TaskMetricGroup taskMetricGroup = operatorMetricGroup.parent();
		MetricGroup metricGroup = taskMetricGroup.getGroup("buffers");
		Gauge<Float> gauge = (Gauge<Float>) metricGroup.getMetric("outPoolUsage");
		if (gauge != null && gauge.getValue() != null) {
			outPoolUsage = gauge.getValue().floatValue();
			this.preAggregateController.getOutPoolUsageHistogram().update((long) (outPoolUsage * 100));
		}

		if (this.controller) {
			// update metrics to the PI Controller
			this.preAggregateController.updateMonitoredValues(latency, outPoolUsage);
		}
	}
}
