package org.apache.flink.streaming.api.functions.aggregation;

import java.io.Serializable;

public interface PreAggregateTrigger<T> extends Serializable {

	/**
	 * Register a callback which will be called once this trigger decides to finish this bundle.
	 */
	void registerCallback(PreAggregateTriggerCallback callback);

	/**
	 * Called for every element that gets added to the merge. If the trigger decides to start
	 * evaluate the input, {@link PreAggregateTriggerCallback#collect()} should be invoked.
	 *
	 * @param element The element that arrived.
	 */
	void onElement(final T element) throws Exception;

	/**
	 * Reset the trigger to its initiate status.
	 */
	void reset();

	String explain();

	long getPeriodSeconds();
}
