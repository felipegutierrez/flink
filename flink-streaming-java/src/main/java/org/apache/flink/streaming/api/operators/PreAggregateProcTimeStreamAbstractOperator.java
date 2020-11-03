package org.apache.flink.streaming.api.operators;

import org.apache.flink.api.common.functions.PreAggregateFunction;
import org.apache.flink.api.common.functions.util.FunctionUtils;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.JobManagerOptions;
import org.apache.flink.dropwizard.metrics.DropwizardHistogramWrapper;
import org.apache.flink.metrics.Gauge;
import org.apache.flink.metrics.Histogram;
import org.apache.flink.streaming.api.functions.aggregation.PreAggregateMonitor;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.runtime.tasks.ProcessingTimeCallback;
import org.apache.flink.streaming.util.functions.PreAggParamGauge;
import org.apache.flink.util.Collector;

import com.codahale.metrics.SlidingTimeWindowArrayReservoir;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.apache.flink.util.Preconditions.checkNotNull;

public abstract class PreAggregateProcTimeStreamAbstractOperator<K, V, IN, OUT>
	extends AbstractStreamOperator<OUT>
	implements OneInputStreamOperator<IN, OUT>, ProcessingTimeCallback {

	// @formatter:off
	// private static final SimpleDateFormat sdf = new SimpleDateFormat("yyyy.MM.dd.HH.mm.ss");
	private static final long serialVersionUID = 1L;
	/** metrics to monitor the PreAggregate operator */
	private final String PRE_AGGREGATE_OUT_POOL_USAGE_HISTOGRAM = "pre-aggregate-outPoolUsage-histogram";
	private final String PRE_AGGREGATE_PARAMETER = "pre-aggregate-parameter";
	/** The function used to process when receiving element. */
	private final PreAggregateFunction<K, V, IN, OUT> function;
	/** processing time to trigger the preAggregate function*/
	private final long intervalMs;
	/** controller properties */
	private final boolean enableController;
	private transient long currentWatermark;
	/** The map in heap to store elements. */
	private transient Map<K, V> bundle;
	/** Output for stream records. */
	private transient Collector<OUT> collector;
	/** The PreAggregate PI controller */
	private PreAggregateMonitor preAggregateMonitor;
	// @formatter:on

	public PreAggregateProcTimeStreamAbstractOperator(
		PreAggregateFunction<K, V, IN, OUT> function,
		long intervalMs, boolean enableController) {
		this.function = checkNotNull(function, "function is null");
		this.intervalMs = intervalMs;
		this.enableController = enableController;
	}

	@Override
	public void open() throws Exception {
		super.open();

		this.collector = new TimestampedCollector<>(output);
		this.bundle = new HashMap<>();

		currentWatermark = 0;

		long now = getProcessingTimeService().getCurrentProcessingTime();
		getProcessingTimeService().registerTimer(now + intervalMs, this);

		// Find the JobManager address
		// ConfigOption<String> jobManagerAddressConfig = ConfigOptions.key("jobmanager.rpc.address").stringType().noDefaultValue();
		// String jobManagerAddress = this.getRuntimeContext().getTaskEnvironment().getTaskManagerInfo().getConfiguration().getValue(jobManagerAddressConfig);
		// String jobManagerAddress = "127.0.0.1";
		String jobManagerAddress = getRuntimeContext().getTaskManagerRuntimeInfo().getConfiguration().getValue(JobManagerOptions.ADDRESS);
		System.out.println("jobManagerAddress: " + jobManagerAddress);

		// report marker metric
		// getRuntimeContext().getMetricGroup().gauge("currentBatch", (Gauge<Long>) () -> currentWatermark);

		// Metrics to send to the controller
		int reservoirWindow = 30;
		com.codahale.metrics.Histogram dropwizardOutPoolBufferHistogram = new com.codahale.metrics.Histogram(
			new SlidingTimeWindowArrayReservoir(reservoirWindow, TimeUnit.SECONDS));
		Histogram outPoolUsageHistogram = getRuntimeContext().getMetricGroup().histogram(
			PRE_AGGREGATE_OUT_POOL_USAGE_HISTOGRAM,
			new DropwizardHistogramWrapper(dropwizardOutPoolBufferHistogram));
		PreAggParamGauge preAggParamGauge = getRuntimeContext()
			.getMetricGroup()
			.gauge(PRE_AGGREGATE_PARAMETER, new PreAggParamGauge());

		// initiate the Controller-monitor with the histogram metrics for each pre-aggregate operator instance
		int subtaskId = getRuntimeContext().getIndexOfThisSubtask();
		this.preAggregateMonitor = new PreAggregateMonitor(
			outPoolUsageHistogram,
			preAggParamGauge,
			jobManagerAddress,
			subtaskId,
			this.enableController);
		this.preAggregateMonitor.start();
	}

	@Override
	public void processElement(StreamRecord<IN> element) throws Exception {
		// get the key and value for the map bundle
		final IN input = element.getValue();
		final K bundleKey = getKey(input);
		final V bundleValue = this.bundle.get(bundleKey);

		// get a new value after adding this element to bundle
		final V newBundleValue = this.function.addInput(bundleValue, input);

		// update to map bundle
		this.bundle.put(bundleKey, newBundleValue);
	}

	/**
	 * Get the key for current processing element, which will be used as the map bundle's key.
	 */
	protected abstract K getKey(final IN input) throws Exception;

	@Override
	public void onProcessingTime(long timestamp) throws Exception {
		long currentProcessingTime = getProcessingTimeService().getCurrentProcessingTime();
		this.collect();
		getProcessingTimeService().registerTimer(currentProcessingTime + intervalMs, this);
		// System.out.println(PreAggregateProcTimeStreamAbstractOperator.class.getSimpleName() + ".onProcessingTime: " + sdf.format(new Timestamp(System.currentTimeMillis())));
	}

	private void collect() throws Exception {
		if (!this.bundle.isEmpty()) {
			this.function.collect(bundle, collector);
			this.bundle.clear();
		}
	}

	@Override
	public void close() throws Exception {
		try {
			this.collect();
		} finally {
			Exception exception = null;

			try {
				super.close();
				if (function != null) {
					FunctionUtils.closeFunction(function);
				}
			} catch (InterruptedException interrupted) {
				exception = interrupted;

				Thread.currentThread().interrupt();
			} catch (Exception e) {
				exception = e;
			}

			if (exception != null) {
				LOG.warn("Errors occurred while closing the BundleOperator.", exception);
			}
		}
	}
}
