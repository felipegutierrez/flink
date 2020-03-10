package org.apache.flink.streaming.api.functions.aggregation;

/**
 * Strategies to pre-aggregate items.
 * <p>
 * GLOBAL: all the pre-aggregate operators use the same threshold parameter to decide when emit tuples to the shuffle phase.
 * LOCAL: each pre-aggregate operator can use a different threshold parameter to decide when emit tuples to the shuffle phase.
 * PER_KEY: the threshold parameter to decide when emit tuples to the shuffle phase is based on the key range according
 * to the key-distribution of the shuffle phase. In this configuration each single physical instance of operators might
 * have different parameters to emit tuples based on the key distribution.
 * </p>
 */
public enum PreAggregateStrategy {
	GLOBAL("GLOBAL"), LOCAL("LOCAL"), PER_KEY("PER_KEY");

	String value;

	PreAggregateStrategy(String value) {
		this.value = value;
	}

	public String getValue() {
		return this.value;
	}
}
