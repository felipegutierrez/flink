package org.apache.flink.api.common.functions;

public interface CombinerTriggerCallback {
	/**
	 * This method is invoked to finish current bundle and start a new one when the
	 * trigger was fired.
	 *
	 * @throws Exception This method may throw exceptions. Throwing an exception
	 *                   will cause the operation to fail and may trigger recovery.
	 */
	void finishBundle() throws Exception;
}
