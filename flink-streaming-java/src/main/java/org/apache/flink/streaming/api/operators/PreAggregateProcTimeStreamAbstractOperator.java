package org.apache.flink.streaming.api.operators;

import org.apache.flink.api.common.functions.PreAggregateFunction;
import org.apache.flink.api.common.functions.util.FunctionUtils;
import org.apache.flink.metrics.Gauge;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.runtime.tasks.ProcessingTimeCallback;
import org.apache.flink.util.Collector;

import java.util.HashMap;
import java.util.Map;

import static org.apache.flink.util.Preconditions.checkNotNull;

public abstract class PreAggregateProcTimeStreamAbstractOperator<K, V, IN, OUT>
	extends AbstractStreamOperator<OUT>
	implements OneInputStreamOperator<IN, OUT>, ProcessingTimeCallback {

	private static final long serialVersionUID = 1L;
	// private static final SimpleDateFormat sdf = new SimpleDateFormat("yyyy.MM.dd.HH.mm.ss");

	private final long intervalMs;
	/** The function used to process when receiving element. */
	private final PreAggregateFunction<K, V, IN, OUT> function;
	/** controller properties */
	private final boolean enableController;
	private transient long currentWatermark;
	/** The map in heap to store elements. */
	private transient Map<K, V> bundle;
	/** Output for stream records. */
	private transient Collector<OUT> collector;

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

		// report marker metric
		getRuntimeContext()
			.getMetricGroup()
			.gauge("currentBatch", (Gauge<Long>) () -> currentWatermark);
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
