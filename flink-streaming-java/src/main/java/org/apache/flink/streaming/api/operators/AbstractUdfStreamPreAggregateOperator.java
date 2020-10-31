package org.apache.flink.streaming.api.operators;

import com.codahale.metrics.SlidingTimeWindowArrayReservoir;
import org.apache.flink.api.common.functions.PreAggregateFunction;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.dropwizard.metrics.DropwizardHistogramWrapper;
import org.apache.flink.metrics.Gauge;
import org.apache.flink.metrics.Histogram;
import org.apache.flink.metrics.MeterView;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.runtime.metrics.groups.OperatorMetricGroup;
import org.apache.flink.runtime.metrics.groups.TaskMetricGroup;
import org.apache.flink.streaming.api.functions.aggregation.*;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.functions.PreAggLatencyMeanGauge;
import org.apache.flink.streaming.util.functions.PreAggParamGauge;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.apache.flink.util.Preconditions.checkNotNull;

public abstract class AbstractUdfStreamPreAggregateOperator<K, V, IN, OUT>
	extends AbstractUdfStreamOperator<OUT, PreAggregateFunction<K, V, IN, OUT>>
	implements OneInputStreamOperator<IN, OUT>, PreAggregateTriggerCallback {

	private static final long serialVersionUID = 1L;
	private final String PRE_AGGREGATE_LATENCY_MEAN = "pre-aggregate-latency-mean";
	private final String PRE_AGGREGATE_LATENCY_HISTOGRAM = "pre-aggregate-latency-histogram";
	private final String PRE_AGGREGATE_OUT_POOL_USAGE_HISTOGRAM = "pre-aggregate-outPoolUsage-histogram";
	private final String PRE_AGGREGATE_PARAMETER = "pre-aggregate-parameter";

	/**
	 * The map in heap to store elements.
	 */
	private final Map<K, V> bundle;

	/**
	 * The trigger that determines how many elements should be put into a bundle.
	 */
	private final PreAggregateTriggerFunction<IN> preAggregateTriggerFunction;
	private final boolean enableController;
	private PreAggregateTimer preAggregateTimer;

	/**
	 * Output for stream records.
	 */
	private transient TimestampedCollector<OUT> collector;
	private transient int numOfElements;
	/**
	 * A Mqtt topic to change the parameter K of pre-aggregating items
	 */
	private PreAggregateParameterListener preAggregateMqttListener;
	/**
	 * A Feedback loop Controller to find the optimal parameter K of pre-aggregating items
	 */
	private PreAggregateMonitor preAggregateMonitor;
	private long elapsedTime;

	/**
	 * The ID of this subTask
	 */
	private int subtaskIndex;

	private String mqttBrokerHost;

	/**
	 * @param function
	 * @param preAggregateTriggerFunction
	 */
	public AbstractUdfStreamPreAggregateOperator(PreAggregateFunction<K, V, IN, OUT> function,
												 PreAggregateTriggerFunction<IN> preAggregateTriggerFunction,
												 boolean enableController) {
		super(function);
		this.chainingStrategy = ChainingStrategy.ALWAYS;
		this.enableController = enableController;
		this.bundle = new HashMap<>();
		this.preAggregateTriggerFunction = checkNotNull(preAggregateTriggerFunction, "bundleTrigger is null");
	}

	@Override
	public void open() throws Exception {
		super.open();

		int reservoirWindow = 30;
		this.numOfElements = 0;
		this.collector = new TimestampedCollector<>(output);

		this.preAggregateTriggerFunction.registerCallback(this);
		// reset trigger
		this.preAggregateTriggerFunction.reset();

		this.subtaskIndex = getRuntimeContext().getIndexOfThisSubtask();
		this.elapsedTime = System.currentTimeMillis();

		// create histogram metrics
		com.codahale.metrics.Histogram dropwizardLatencyHistogram = new com.codahale.metrics.Histogram(new SlidingTimeWindowArrayReservoir(reservoirWindow, TimeUnit.SECONDS));
		Histogram latencyHistogram = getRuntimeContext().getMetricGroup().histogram(
			PRE_AGGREGATE_LATENCY_HISTOGRAM, new DropwizardHistogramWrapper(dropwizardLatencyHistogram));
		com.codahale.metrics.Histogram dropwizardOutPoolBufferHistogram = new com.codahale.metrics.Histogram(new SlidingTimeWindowArrayReservoir(reservoirWindow, TimeUnit.SECONDS));
		Histogram outPoolUsageHistogram = getRuntimeContext().getMetricGroup().histogram(
			PRE_AGGREGATE_OUT_POOL_USAGE_HISTOGRAM, new DropwizardHistogramWrapper(dropwizardOutPoolBufferHistogram));
		PreAggParamGauge preAggParamGauge = getRuntimeContext().getMetricGroup().gauge(PRE_AGGREGATE_PARAMETER, new PreAggParamGauge());
		PreAggLatencyMeanGauge preAgglatencyMeanGauge = getRuntimeContext().getMetricGroup().gauge(PRE_AGGREGATE_LATENCY_MEAN, new PreAggLatencyMeanGauge());

		ConfigOption<String> restAddressOption = ConfigOptions
			.key("rest.address")
			.stringType()
			.noDefaultValue();
		// String restAddress = this.getRuntimeContext().getTaskEnvironment().getTaskManagerInfo().getConfiguration().getValue(restAddressOption);
		String restAddress = "127.0.0.1";
		System.out.println("rest.address: " + restAddress);

		// initiate the Controller with the histogram metrics
		this.preAggregateMonitor = new PreAggregateMonitor(this.preAggregateTriggerFunction, latencyHistogram,
			outPoolUsageHistogram, preAggParamGauge, preAgglatencyMeanGauge, restAddress, this.subtaskIndex, this.enableController);

		try {
			if (this.preAggregateTriggerFunction.getPreAggregateStrategy() == PreAggregateStrategy.GLOBAL) {
				this.preAggregateMqttListener = new PreAggregateParameterListener(this.preAggregateTriggerFunction, restAddress, this.subtaskIndex);
			} else if (this.preAggregateTriggerFunction.getPreAggregateStrategy() == PreAggregateStrategy.LOCAL) {
				this.preAggregateMqttListener = new PreAggregateParameterListener(this.preAggregateTriggerFunction, restAddress, this.subtaskIndex);
			} else if (this.preAggregateTriggerFunction.getPreAggregateStrategy() == PreAggregateStrategy.PER_KEY) {
				System.out.println("Pre-aggregate per-key strategy not implemented.");
			} else {
				System.out.println("Pre-aggregate strategy not implemented.");
			}
			this.preAggregateMqttListener.connect();
			this.preAggregateMqttListener.start();

			this.preAggregateMonitor.connect();
			this.preAggregateMonitor.start();
		} catch (Exception e) {
			e.printStackTrace();
		}

		// initiate the pre-aggregate by time only if the controller is not enable and the max time is different of -1
		if (!this.enableController &&
			this.preAggregateTriggerFunction.getMaxTime() > 0) {
			this.preAggregateTimer = new PreAggregateTimer(this.preAggregateTriggerFunction, this.subtaskIndex);
			this.preAggregateTimer.start();
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
		this.preAggregateTriggerFunction.onElement(input);
	}

	protected abstract K getKey(final IN input) throws Exception;

	@Override
	public void collect() throws Exception {
		if (!this.bundle.isEmpty()) {
			this.numOfElements = 0;
			this.userFunction.collect(bundle, collector);
			this.bundle.clear();
		}
		this.preAggregateTriggerFunction.reset();

		// update metrics to Prometheus+Grafana and reset latency elapsed
		long latency = System.currentTimeMillis() - elapsedTime;
		this.elapsedTime = System.currentTimeMillis();
		this.preAggregateMonitor.getLatencyHistogram().update(latency);

		// update outPoolUsage size metrics to Prometheus+Grafana
		float outPoolUsage = 0.0f;
		OperatorMetricGroup operatorMetricGroup = (OperatorMetricGroup) this.getMetricGroup();
		TaskMetricGroup taskMetricGroup = operatorMetricGroup.parent();
		MetricGroup metricGroup = taskMetricGroup.getGroup("buffers");
//		Gauge<Float> gaugeOutPoolUsage = (Gauge<Float>) metricGroup.getMetric("outPoolUsage");
//		if (gaugeOutPoolUsage != null && gaugeOutPoolUsage.getValue() != null) {
//			outPoolUsage = gaugeOutPoolUsage.getValue().floatValue();
//			this.preAggregateMonitor.getOutPoolUsageHistogram().update((long) (outPoolUsage * 100));
//		}
//
//		MeterView meterNumRecordsOutPerSecond = (MeterView) taskMetricGroup.getMetric("numRecordsOutPerSecond");
//		MeterView meterNumRecordsInPerSecond = (MeterView) taskMetricGroup.getMetric("numRecordsInPerSecond");
//		this.preAggregateMonitor.setNumRecordsOutPerSecond(meterNumRecordsOutPerSecond.getRate());
//		this.preAggregateMonitor.setNumRecordsInPerSecond(meterNumRecordsInPerSecond.getRate());
	}
}
