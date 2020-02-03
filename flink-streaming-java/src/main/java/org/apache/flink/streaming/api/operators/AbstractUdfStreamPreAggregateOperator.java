package org.apache.flink.streaming.api.operators;

import org.apache.flink.api.common.functions.PreAggregateFunction;
import org.apache.flink.streaming.api.functions.aggregation.PreAggregateMqttListener;
import org.apache.flink.streaming.api.functions.aggregation.PreAggregateTriggerCallback;
import org.apache.flink.streaming.api.functions.aggregation.PreAggregateTriggerFunction;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;

import java.util.HashMap;
import java.util.Map;

import static org.apache.flink.util.Preconditions.checkNotNull;

public abstract class AbstractUdfStreamPreAggregateOperator<K, V, IN, OUT>
	extends AbstractUdfStreamOperator<OUT, PreAggregateFunction<K, V, IN, OUT>>
	implements OneInputStreamOperator<IN, OUT>, PreAggregateTriggerCallback {

	private static final long serialVersionUID = 1L;

	/**
	 * The map in heap to store elements.
	 */
	private final Map<K, V> bundle;

	/**
	 * The trigger that determines how many elements should be put into a bundle.
	 */
	private final PreAggregateTriggerFunction<IN> preAggregateTrigger;

	private PreAggregateMqttListener preAggregateMqttListener;

	/**
	 * Output for stream records.
	 */
	private transient TimestampedCollector<OUT> collector;
	private transient int numOfElements;

	public AbstractUdfStreamPreAggregateOperator(PreAggregateFunction<K, V, IN, OUT> function,
												 PreAggregateTriggerFunction<IN> preAggregateTrigger) {
		super(function);
		this.chainingStrategy = ChainingStrategy.ALWAYS;
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
		try {
			this.preAggregateMqttListener = new PreAggregateMqttListener(this.preAggregateTrigger);
			this.preAggregateMqttListener.connect();
			this.preAggregateMqttListener.start();
		} catch (Exception e) {
			e.printStackTrace();
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
	}
}
