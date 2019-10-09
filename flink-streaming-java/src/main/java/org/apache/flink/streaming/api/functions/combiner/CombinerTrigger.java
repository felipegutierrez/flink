package org.apache.flink.streaming.api.functions.combiner;

import java.io.Serializable;

public interface CombinerTrigger<T> extends Serializable {

	/**
	 * Register a callback which will be called once this trigger decides to finish this bundle.
	 */
	void registerCallback(CombinerTriggerCallback callback);

	/**
	 * Called for every element that gets added to the merge. If the trigger decides to start
	 * evaluate the input, {@link CombinerTriggerCallback#finishMerge()} should be invoked.
	 *
	 * @param element The element that arrived.
	 */
	void onElement(final T element) throws Exception;

	/**
	 * Reset the trigger to its initiate status.
	 */
	void reset();

	String explain();
}
