package org.apache.flink.streaming.api.functions.aggregation;

public interface PreAggregateTriggerCallback {
	/**
	 * This method is invoked to finish current merge and start a new one when the
	 * trigger was fired.
	 *
	 * @throws Exception This method may throw exceptions. Throwing an exception
	 *                   will cause the operation to fail and may trigger recovery.
	 */
	void collect() throws Exception;
}
