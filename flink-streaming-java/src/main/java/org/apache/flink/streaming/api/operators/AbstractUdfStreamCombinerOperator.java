package org.apache.flink.streaming.api.operators;

import org.apache.flink.api.common.functions.CombinerFunction;
import org.apache.flink.api.common.functions.CombinerTrigger;
import org.apache.flink.api.common.functions.CombinerTriggerCallback;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

import static org.apache.flink.util.Preconditions.checkNotNull;

public abstract class AbstractUdfStreamCombinerOperator<K, V, IN, OUT> extends AbstractUdfStreamOperator<OUT, CombinerFunction<K, V, IN, OUT>>
	implements OneInputStreamOperator<IN, OUT>, CombinerTriggerCallback {

	private static final Logger logger = LoggerFactory.getLogger(AbstractUdfStreamCombinerOperator.class);
	private static final long serialVersionUID = 1L;

	/**
	 * The map in heap to store elements.
	 */
	private final Map<K, V> bundle;

	/**
	 * The trigger that determines how many elements should be put into a bundle.
	 */
	private final CombinerTrigger<IN> combinerTrigger;

	/**
	 * Output for stream records.
	 */
	private transient TimestampedCollector<OUT> collector;

	private transient int numOfElements = 0;

	public AbstractUdfStreamCombinerOperator(CombinerFunction<K, V, IN, OUT> function, CombinerTrigger<IN> combinerTrigger) {
		super(function);
		chainingStrategy = ChainingStrategy.ALWAYS;
		this.bundle = new HashMap<>();
		this.combinerTrigger = checkNotNull(combinerTrigger, "bundleTrigger is null");
	}

	@Override
	public void open() throws Exception {
		super.open();

		numOfElements = 0;
		collector = new TimestampedCollector<>(output);

		combinerTrigger.registerCallback(this);
		// reset trigger
		combinerTrigger.reset();
	}

	@Override
	public void processElement(StreamRecord<IN> element) throws Exception {
		// get the key and value for the map bundle
		final IN input = element.getValue();
		final K bundleKey = getKey(input);
		final V bundleValue = this.bundle.get(bundleKey);

		// get a new value after adding this element to bundle
		final V newBundleValue = userFunction.addInput(bundleValue, input);

		// update to map bundle
		bundle.put(bundleKey, newBundleValue);

		numOfElements++;
		combinerTrigger.onElement(input);
	}

	protected abstract K getKey(final IN input) throws Exception;

	@Override
	public void finishMerge() throws Exception {
		if (!bundle.isEmpty()) {
			numOfElements = 0;
			userFunction.finishMerge(bundle, collector);
			bundle.clear();
		}
		combinerTrigger.reset();
	}
}
