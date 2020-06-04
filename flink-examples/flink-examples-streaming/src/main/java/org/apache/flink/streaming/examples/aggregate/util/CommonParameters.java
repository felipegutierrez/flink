package org.apache.flink.streaming.examples.aggregate.util;

public class CommonParameters {
	// operator names
	public static final String OPERATOR_SOURCE = "source";
	public static final String OPERATOR_TOKENIZER = "tokenizer";
	public static final String OPERATOR_REDUCER = "reducer";
	public static final String OPERATOR_PRE_AGGREGATE = "pre-aggregate";
	public static final String OPERATOR_FLAT_OUTPUT = "flat-output";
	public static final String OPERATOR_SINK = "sink";

	// source parameters
	public static final String SOURCE = "input";
	public static final String TOPIC_DATA_SOURCE = "topic-data-source";
	public static final String SOURCE_DATA_MQTT = "mqtt";
	public static final String SOURCE_HOST = "sourceHost";
	public static final String SOURCE_PORT = "sourcePort";
	public static final String SOURCE_WORDS = "words";
	public static final String SOURCE_SKEW_WORDS = "skew";
	public static final String SOURCE_FEW_WORDS = "few";
	public static final String SOURCE_DATA_RATE_VARIATION_WORDS = "variation";
	public static final String SOURCE_DATA_HAMLET = "hamlet";
	public static final String SOURCE_DATA_MOBY_DICK = "mobydick";
	public static final String SOURCE_DATA_DICTIONARY = "dictionary";
	// sink parameters
	public static final String SINK_HOST = "sinkHost";
	public static final String SINK_PORT = "sinkPort";
	public static final String SINK = "output";
	public static final String SINK_DATA_MQTT = "mqtt";
	public static final String SINK_LOG = "log";
	public static final String SINK_TEXT = "text";
	public static final String TOPIC_DATA_SINK = "topic-data-sink";
	// other parameters
	public static final String PARALLELISM_GROUP_01 = "parallelism-group-01";
	public static final String PARALLELISM_GROUP_02 = "parallelism-group-02";
	public static final String SLOT_GROUP_DEFAULT = "default";
	public static final String SLOT_GROUP_01_01 = "group-01-01";
	public static final String SLOT_GROUP_01_02 = "group-01-02";
	public static final String SLOT_GROUP_02_01 = "group-02-01";
	public static final String SLOT_GROUP_02_02 = "group-02-02";
	public static final String SLOT_GROUP_02 = "group-02";
	public static final String PRE_AGGREGATE_WINDOW = "pre-aggregate-window";
	public static final String PRE_AGGREGATE_STRATEGY = "strategy";
	public static final String SLOT_GROUP_SPLIT = "slotSplit";
	public static final String DISABLE_OPERATOR_CHAINING = "disableOperatorChaining";
	public static final String CONTROLLER = "controller";
	public static final String LATENCY_TRACKING_INTERVAL = "latencyTrackingInterval";
	public static final String SIMULATE_SKEW = "simulateSkew";
	public static final String WINDOW = "window";
	public static final String SYNTHETIC_DELAY = "delay";
	public static final String BUFFER_TIMEOUT = "bufferTimeout";
	public static final String POOLING_FREQUENCY = "pooling";
	public static final String TOP_N = "topN";
}
