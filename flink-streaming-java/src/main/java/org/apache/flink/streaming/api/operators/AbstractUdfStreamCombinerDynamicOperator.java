package org.apache.flink.streaming.api.operators;

import org.apache.flink.api.common.functions.CombinerDynamicTrigger;
import org.apache.flink.api.common.functions.CombinerFunction;
import org.apache.flink.api.common.functions.CombinerTriggerCallback;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

import static org.apache.flink.util.Preconditions.checkNotNull;

public abstract class AbstractUdfStreamCombinerDynamicOperator<K, V, IN, OUT> extends AbstractUdfStreamOperator<OUT, CombinerFunction<K, V, IN, OUT>>
	implements OneInputStreamOperator<IN, OUT>, CombinerTriggerCallback {

	private static final Logger logger = LoggerFactory.getLogger(AbstractUdfStreamCombinerDynamicOperator.class);

	/**
	 * The map in heap to store elements.
	 */
	private final Map<K, V> bundle;

	/**
	 * The trigger that determines how many elements should be put into a bundle.
	 */
	private final CombinerDynamicTrigger<K, IN> combinerTrigger;

	/**
	 * Output for stream records.
	 */
	private transient TimestampedCollector<OUT> collector;

	private transient int numOfElements = 0;

	public AbstractUdfStreamCombinerDynamicOperator(CombinerFunction<K, V, IN, OUT> function,
													CombinerDynamicTrigger<K, IN> bundleTrigger) {
		super(function);
		this.chainingStrategy = ChainingStrategy.ALWAYS;
		this.bundle = new HashMap<>();
		this.combinerTrigger = checkNotNull(bundleTrigger, "bundleTrigger is null");
	}

	@Override
	public void open() throws Exception {
		super.open();

		this.numOfElements = 0;
		this.collector = new TimestampedCollector<>(output);

		this.combinerTrigger.registerCallback(this);
		// reset trigger
		this.combinerTrigger.reset();
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
		this.combinerTrigger.onElement(bundleKey, input);
	}

	protected abstract K getKey(final IN input) throws Exception;

	@Override
	public void finishMerge() throws Exception {
		if (!this.bundle.isEmpty()) {
			this.numOfElements = 0;
			this.userFunction.finishMerge(bundle, collector);
			this.bundle.clear();
		}
		this.combinerTrigger.reset();
	}
}
