package org.apache.flink.streaming.api.operators;

import org.apache.flink.api.common.functions.CombinerFunction;
import org.apache.flink.api.common.functions.CombinerTrigger;
import org.apache.flink.api.java.functions.KeySelector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class StreamCombinerOperator<K, V, IN, OUT> extends AbstractUdfStreamCombinerOperator<K, V, IN, OUT> {
	private static final long serialVersionUID = 1L;
	private static final Logger logger = LoggerFactory.getLogger(StreamCombinerOperator.class);

	/**
	 * KeySelector is used to extract key for bundle map.
	 */
	private final KeySelector<IN, K> keySelector;

	public StreamCombinerOperator(CombinerFunction<K, V, IN, OUT> function,
								  CombinerTrigger<IN> combinerTrigger,
								  KeySelector<IN, K> keySelector) {
		super(function, combinerTrigger);
		this.keySelector = keySelector;
	}

	@Override
	protected K getKey(IN input) throws Exception {
		return this.keySelector.getKey(input);
	}
}
