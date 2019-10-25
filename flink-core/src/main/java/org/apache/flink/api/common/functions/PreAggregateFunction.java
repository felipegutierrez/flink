package org.apache.flink.api.common.functions;

import org.apache.flink.annotation.Public;
import org.apache.flink.util.Collector;

import javax.annotation.Nullable;
import java.util.Map;

/**
 * @param <K>
 * @param <V>
 * @param <T> Type of the input elements.
 * @param <O> Type of the output elements.
 */
@Public
public abstract class PreAggregateFunction<K, V, T, O> implements Function {
	private static final long serialVersionUID = 1L;

	/**
	 * Adds the given input to the given value, returning the new bundle value.
	 *
	 * @param value the existing bundle value, maybe null
	 * @param input the given input, not null
	 * @throws Exception
	 */
	public abstract V addInput(@Nullable V value, T input) throws Exception;

	/**
	 * Called when a merge is finished. Transform a bundle to zero, one, or more
	 * output elements.
	 */
	public abstract void finishMerge(Map<K, V> buffer, Collector<O> out) throws Exception;
}
