package org.apache.flink.api.common.functions;

import java.io.Serializable;

public interface CombinerTriggerDynamic<K, T> extends Serializable {
	/**
	 * Register a callback which will be called once this trigger decides to finish
	 * this bundle.
	 */
	void registerCallback(CombinerTriggerCallback callback);

	/**
	 * Called for every element that gets added to the bundle. If the trigger
	 * decides to start evaluate the input,
	 * {@link CombinerTriggerCallback#finishBundle()} should be invoked.
	 *
	 * @param element The element that arrived.
	 */
	void onElement(final K key, final T element) throws Exception;

	/**
	 * Reset the trigger to its initiate status.
	 */
	void reset() throws Exception;

	String explain();
}
