package org.apache.flink.streaming.examples.aggregate;

import io.airlift.tpch.LineItem;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.PreAggregateConcurrentFunction;
import org.apache.flink.api.common.functions.PreAggregateFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple11;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.examples.aggregate.util.LineItemSource;
import org.apache.flink.streaming.examples.aggregate.util.MqttDataSink;
import org.apache.flink.streaming.examples.utils.DataRateListener;
import org.apache.flink.util.Collector;

import javax.annotation.Nullable;
import java.util.Map;
import java.util.concurrent.ConcurrentMap;

import static org.apache.flink.streaming.examples.aggregate.util.CommonParameters.*;

/**
 * 1 Q1 - Pricing Summary Report Query
 * https://docs.deistercloud.com/content/Databases.30/TPCH%20Benchmark.90/Sample%20querys.20.xml
 *
 * <pre>
 * select
 *        l_returnflag,
 *        l_linestatus,
 *        sum(l_quantity) as sum_qty,
 *        sum(l_extendedprice) as sum_base_price,
 *        sum(l_extendedprice * (1-l_discount)) as sum_disc_price,
 *        sum(l_extendedprice * (1-l_discount) * (1+l_tax)) as sum_charge,
 *        avg(l_quantity) as avg_qty,
 *        avg(l_extendedprice) as avg_price,
 *        avg(l_discount) as avg_disc,
 *        count(*) as count_order
 *  from
 *        lineitem
 *  where
 *        l_shipdate <= mdy (12, 01, 1998 ) - 90 units day
 *  group by
 *        l_returnflag,
 *        l_linestatus
 *  order by
 *        l_returnflag,
 *        l_linestatus;
 * </pre>
 * <p>
 * /bin/flink run TPCHQuery01PreAggregate.jar -pre-aggregate-window 1 -output mqtt -sinkHost 130.239.48.135
 * /bin/flink run TPCHQuery01PreAggregate.jar -pre-aggregate-window 1 -output mqtt -sinkHost 130.239.48.135 -slotSplit 1 -parallelism-group-01 16 -parallelism-group-02 16
 * /bin/flink run TPCHQuery01PreAggregate.jar -pre-aggregate-window 1 -output mqtt -sinkHost 130.239.48.135 -slotSplit 1 -parallelism-group-02 16
 * /bin/flink run TPCHQuery01PreAggregate.jar -pre-aggregate-window 1 -output mqtt -sinkHost 130.239.48.135 -slotSplit 1 -parallelism-group-02 24
 */
public class TPCHQuery01PreAggregate {
	public static void main(String[] args) throws Exception {
		ParameterTool params = ParameterTool.fromArgs(args);
		final String input = params.get(SOURCE, TPCH_DATA_LINE_ITEM);
		String sinkHost = params.get(SINK_HOST, "127.0.0.1");
		int sinkPort = params.getInt(SINK_PORT, 1883);
		String output = params.get(SINK, SINK_TEXT);
		int maxCount = params.getInt(MAX_COUNT_SOURCE, -1);
		int preAggregationWindowCount = params.getInt(PRE_AGGREGATE_WINDOW, 1);
		long preAggregationWindowTimer = params.getLong(PRE_AGGREGATE_WINDOW_TIMEOUT, -1);
		int slotSplit = params.getInt(SLOT_GROUP_SPLIT, 0);
		int parallelisGroup02 = params.getInt(PARALLELISM_GROUP_02, ExecutionConfig.PARALLELISM_DEFAULT);
		boolean enableCombiner = params.getBoolean(COMBINER, true);
		boolean enableController = params.getBoolean(CONTROLLER, true);
		boolean disableOperatorChaining = params.getBoolean(DISABLE_OPERATOR_CHAINING, false);

		System.out.println("Download data from:");
		System.out.println("https://docs.deistercloud.com/content/Databases.30/TPCH%20Benchmark.90/Data%20generation%20tool.30.xml");
		System.out.println("data source                                             : " + input);
		System.out.println("max times to read the source                            : " + maxCount);
		System.out.println("data sink                                               : " + output);
		System.out.println("data sink host:port                                     : " + sinkHost + ":" + sinkPort);
		System.out.println("data sink topic                                         : " + TOPIC_DATA_SINK);
		System.out.println("Feedback loop Controller                                : " + enableController);
		System.out.println("Slot split 0-no split, 1-combiner, 2-combiner & reducer : " + slotSplit);
		System.out.println("Disable operator chaining                               : " + disableOperatorChaining);
		System.out.println("Enable combiner                                         : " + enableCombiner);
		System.out.println("pre-aggregate window [count]                            : " + preAggregationWindowCount);
		System.out.println("pre-aggregate window [seconds]                          : " + preAggregationWindowTimer);
		System.out.println("Parallelism group 02                                    : " + parallelisGroup02);
		System.out.println("Changing pre-aggregation frequency before shuffling:");
		System.out.println("mosquitto_pub -h 127.0.0.1 -p 1883 -t topic-pre-aggregate-parameter -m \"100\"");
		System.out.println(DataRateListener.class.getSimpleName() + " class to read data rate from file [" + DataRateListener.DATA_RATE_FILE + "] in milliseconds.");
		System.out.println("This listener reads every 60 seconds only the first line from the data rate file.");
		System.out.println("Use the following command to change the nanoseconds data rate:");
		System.out.println("1000000 nanoseconds = 1 millisecond and 1000000000 nanoseconds = 1000 milliseconds = 1 second");
		System.out.println("500 nanoseconds   = 2M rec/sec");
		System.out.println("1000 nanoseconds  = 1M rec/sec");
		System.out.println("2000 nanoseconds  = 500K rec/sec");
		System.out.println("5000 nanoseconds  = 200K rec/sec");
		System.out.println("10000 nanoseconds = 100K rec/sec");
		System.out.println("20000 nanoseconds = 50K rec/sec");
		System.out.println("echo \"1000\" > " + DataRateListener.DATA_RATE_FILE);

		// set up streaming execution environment
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		if (disableOperatorChaining) {
			env.disableOperatorChaining();
		}
		String slotGroup01 = SLOT_GROUP_DEFAULT;
		String slotGroup02 = SLOT_GROUP_DEFAULT;
		if (slotSplit == 0) {
			slotGroup01 = SLOT_GROUP_DEFAULT;
			slotGroup02 = SLOT_GROUP_DEFAULT;
		} else if (slotSplit == 1) {
			slotGroup01 = SLOT_GROUP_01_01;
			slotGroup02 = SLOT_GROUP_DEFAULT;
		} else if (slotSplit == 2) {
			slotGroup01 = SLOT_GROUP_01_01;
			slotGroup02 = SLOT_GROUP_01_02;
		}

		DataStream<LineItem> lineItems = env.addSource(new LineItemSource(maxCount)).name(OPERATOR_SOURCE).uid(OPERATOR_SOURCE).slotSharingGroup(slotGroup01);

		DataStream<Tuple2<String, Tuple11<String, String, Long, Double, Double, Double, Double, Long, Double, Double, Long>>> lineItemsMap = lineItems
			.map(new LineItemToTuple11Map()).name(OPERATOR_TOKENIZER).uid(OPERATOR_TOKENIZER).slotSharingGroup(slotGroup01);

		DataStream<Tuple2<String, Tuple11<String, String, Long, Double, Double, Double, Double, Long, Double, Double, Long>>> lineItemsCombined = null;
		if (!enableCombiner) {
			// no combiner
			lineItemsCombined = lineItemsMap;
		} else if (enableController == false && preAggregationWindowTimer > 0) {
			// static combiner based on timeout
			PreAggregateConcurrentFunction<String,
				Tuple11<String, String, Long, Double, Double, Double, Double, Long, Double, Double, Long>,
				Tuple2<String, Tuple11<String, String, Long, Double, Double, Double, Double, Long, Double, Double, Long>>,
				Tuple2<String, Tuple11<String, String, Long, Double, Double, Double, Double, Long, Double, Double, Long>>> lineItemPreAggUDF = new LineItemSumPreAggConcurrent();
			lineItemsCombined = lineItemsMap
				.combiner(lineItemPreAggUDF, preAggregationWindowTimer).disableChaining().name(OPERATOR_PRE_AGGREGATE).uid(OPERATOR_PRE_AGGREGATE).slotSharingGroup(slotGroup01);
		} else {
			PreAggregateFunction<String,
				Tuple11<String, String, Long, Double, Double, Double, Double, Long, Double, Double, Long>,
				Tuple2<String, Tuple11<String, String, Long, Double, Double, Double, Double, Long, Double, Double, Long>>,
				Tuple2<String, Tuple11<String, String, Long, Double, Double, Double, Double, Long, Double, Double, Long>>> lineItemPreAggUDF = new LineItemSumPreAgg();
			lineItemsCombined = lineItemsMap
				.combiner(lineItemPreAggUDF, preAggregationWindowCount, enableController).disableChaining().name(OPERATOR_PRE_AGGREGATE).uid(OPERATOR_PRE_AGGREGATE).slotSharingGroup(slotGroup01);
		}

		DataStream<Tuple2<String, Tuple11<String, String, Long, Double, Double, Double, Double, Long, Double, Double, Long>>> sumAndAvgLineItems = lineItemsCombined
			.keyBy(new LineItemFlagAndStatusKeySelector())
			.reduce(new SumAndAvgLineItemReducer()).name(OPERATOR_REDUCER).uid(OPERATOR_REDUCER).slotSharingGroup(slotGroup02).setParallelism(parallelisGroup02);

		DataStream<String> result = sumAndAvgLineItems
			.map(new Tuple11ToLineItemResult()).name(OPERATOR_FLAT_OUTPUT).uid(OPERATOR_FLAT_OUTPUT).slotSharingGroup(slotGroup02).setParallelism(parallelisGroup02);

		if (output.equalsIgnoreCase(SINK_DATA_MQTT)) {
			result
				.addSink(new MqttDataSink(TOPIC_DATA_SINK, sinkHost, sinkPort)).name(OPERATOR_SINK).uid(OPERATOR_SINK).slotSharingGroup(slotGroup02).setParallelism(parallelisGroup02);
		} else if (output.equalsIgnoreCase(SINK_TEXT)) {
			result
				.print().name(OPERATOR_SINK).uid(OPERATOR_SINK).slotSharingGroup(slotGroup02).setParallelism(parallelisGroup02);
		} else {
			System.out.println("discarding output");
		}

		System.out.println("Execution plan >>>\n" + env.getExecutionPlan());
		env.execute(TPCHQuery01PreAggregate.class.getSimpleName());
	}

	// *************************************************************************
	// GENERIC merge function
	// *************************************************************************

	/**
	 * <pre>
	 *        l_returnflag, // string
	 *        l_linestatus, // string
	 *        sum(l_quantity) as sum_qty, // Long
	 *        sum(l_extendedprice) as sum_base_price, // Double
	 *        sum(l_discount) as sum_disc // Double
	 *        sum(l_extendedprice * (1-l_discount)) as sum_disc_price, // Double
	 *        sum(l_extendedprice * (1-l_discount) * (1+l_tax)) as sum_charge, // Double
	 *        avg(l_quantity) as avg_qty, // long
	 *        avg(l_extendedprice) as avg_price, // Double
	 *        avg(l_discount) as avg_disc, // Double
	 *        count(*) as count_order // long
	 * </pre>
	 */
	private static class LineItemToTuple11Map implements MapFunction<LineItem,
		Tuple2<String, Tuple11<String, String, Long, Double, Double, Double, Double, Long, Double, Double, Long>>> {
		private static final long serialVersionUID = 1L;

		@Override
		public Tuple2<String,
			Tuple11<String, String, Long, Double, Double, Double, Double, Long, Double, Double, Long>> map(LineItem lineItem) throws Exception {

			String key = lineItem.getReturnFlag() + "|" + lineItem.getStatus();
			return Tuple2.of(key, Tuple11.of(
				lineItem.getReturnFlag(), lineItem.getStatus(),
				lineItem.getQuantity(),
				lineItem.getExtendedPrice(),
				lineItem.getDiscount(),
				lineItem.getExtendedPrice() * (1 - lineItem.getDiscount()),
				lineItem.getExtendedPrice() * (1 - lineItem.getDiscount()) * (1 + lineItem.getTax()),
				lineItem.getQuantity(),
				lineItem.getExtendedPrice(),
				lineItem.getDiscount(),
				1L));
		}
	}

	private static class LineItemSumPreAgg extends PreAggregateFunction<String,
		Tuple11<String, String, Long, Double, Double, Double, Double, Long, Double, Double, Long>,
		Tuple2<String, Tuple11<String, String, Long, Double, Double, Double, Double, Long, Double, Double, Long>>,
		Tuple2<String, Tuple11<String, String, Long, Double, Double, Double, Double, Long, Double, Double, Long>>> {
		private static final long serialVersionUID = 1L;

		@Override
		public Tuple11<String, String, Long, Double, Double, Double, Double, Long, Double, Double, Long> addInput(
			@Nullable Tuple11<String, String, Long, Double, Double, Double, Double, Long, Double, Double, Long> value,
			Tuple2<String, Tuple11<String, String, Long, Double, Double, Double, Double, Long, Double, Double, Long>> input) throws Exception {
			if (value == null) {
				return input.f1;
			} else {
				String flag = input.f1.f0;
				String status = input.f1.f1;
				Long sumQty = input.f1.f2 + value.f2;
				Double sumBasePrice = input.f1.f3 + value.f3;
				Double sumDisc = input.f1.f4 + value.f4;
				Double sumDiscPrice = input.f1.f5 + value.f5;
				Double sumCharge = input.f1.f6 + value.f6;
				Long avgQty = 0L;
				Double avgBasePrice = 0.0;
				Double avgDisc = 0.0;
				Long count = input.f1.f10 + value.f10;

				return Tuple11.of(flag, status, sumQty, sumBasePrice, sumDisc, sumDiscPrice, sumCharge, avgQty, avgBasePrice, avgDisc, count);
			}
		}

		@Override
		public void collect(Map<String, Tuple11<String, String, Long, Double, Double, Double, Double, Long, Double, Double, Long>> buffer,
							Collector<Tuple2<String, Tuple11<String, String, Long, Double, Double, Double, Double, Long, Double, Double, Long>>> out) throws Exception {
			for (Map.Entry<String, Tuple11<String, String, Long, Double, Double, Double, Double, Long, Double, Double, Long>> entry : buffer.entrySet()) {
				out.collect(Tuple2.of(entry.getKey(), entry.getValue()));
			}
		}
	}

	private static class LineItemSumPreAggConcurrent extends PreAggregateConcurrentFunction<String,
		Tuple11<String, String, Long, Double, Double, Double, Double, Long, Double, Double, Long>,
		Tuple2<String, Tuple11<String, String, Long, Double, Double, Double, Double, Long, Double, Double, Long>>,
		Tuple2<String, Tuple11<String, String, Long, Double, Double, Double, Double, Long, Double, Double, Long>>> {
		private static final long serialVersionUID = 1L;

		@Override
		public Tuple11<String, String, Long, Double, Double, Double, Double, Long, Double, Double, Long> addInput(
			@Nullable Tuple11<String, String, Long, Double, Double, Double, Double, Long, Double, Double, Long> value,
			Tuple2<String, Tuple11<String, String, Long, Double, Double, Double, Double, Long, Double, Double, Long>> input) throws Exception {
			if (value == null) {
				return input.f1;
			} else {
				String flag = input.f1.f0;
				String status = input.f1.f1;
				Long sumQty = input.f1.f2 + value.f2;
				Double sumBasePrice = input.f1.f3 + value.f3;
				Double sumDisc = input.f1.f4 + value.f4;
				Double sumDiscPrice = input.f1.f5 + value.f5;
				Double sumCharge = input.f1.f6 + value.f6;
				Long avgQty = 0L;
				Double avgBasePrice = 0.0;
				Double avgDisc = 0.0;
				Long count = input.f1.f10 + value.f10;

				return Tuple11.of(flag, status, sumQty, sumBasePrice, sumDisc, sumDiscPrice, sumCharge, avgQty, avgBasePrice, avgDisc, count);
			}
		}

		@Override
		public void collect(ConcurrentMap<String, Tuple11<String, String, Long, Double, Double, Double, Double, Long, Double, Double, Long>> buffer,
							Collector<Tuple2<String, Tuple11<String, String, Long, Double, Double, Double, Double, Long, Double, Double, Long>>> out) throws Exception {
			for (Map.Entry<String, Tuple11<String, String, Long, Double, Double, Double, Double, Long, Double, Double, Long>> entry : buffer.entrySet()) {
				out.collect(Tuple2.of(entry.getKey(), entry.getValue()));
			}
		}
	}

	private static class LineItemFlagAndStatusKeySelector implements KeySelector<Tuple2<String, Tuple11<String, String, Long, Double, Double, Double, Double, Long, Double, Double, Long>>, String> {
		private static final long serialVersionUID = 1L;

		@Override
		public String getKey(Tuple2<String, Tuple11<String, String, Long, Double, Double, Double, Double, Long, Double, Double, Long>> value) throws Exception {
			return value.f0;
		}
	}

	private static class SumAndAvgLineItemReducer implements ReduceFunction<Tuple2<String, Tuple11<String, String, Long, Double, Double, Double, Double, Long, Double, Double, Long>>> {
		private static final long serialVersionUID = 1L;

		@Override
		public Tuple2<String, Tuple11<String, String, Long, Double, Double, Double, Double, Long, Double, Double, Long>> reduce(
			Tuple2<String, Tuple11<String, String, Long, Double, Double, Double, Double, Long, Double, Double, Long>> value1,
			Tuple2<String, Tuple11<String, String, Long, Double, Double, Double, Double, Long, Double, Double, Long>> value2) throws Exception {

			String key = value1.f0;
			Long count = value1.f1.f10 + value2.f1.f10;
			Long sumQty = value1.f1.f2 + value2.f1.f2;
			Double sumBasePrice = value1.f1.f3 + value2.f1.f3;
			Double sumDisc = value1.f1.f4 + value2.f1.f4;
			Double sumDiscPrice = value1.f1.f5 + value2.f1.f5;
			Double sumCharge = value1.f1.f6 + value2.f1.f6;
			Long avgQty = sumQty / count;
			Double avgBasePrice = sumBasePrice / count;
			Double avgDisc = sumDisc / count;

			return Tuple2.of(key,
				Tuple11.of(value1.f1.f0, value1.f1.f1,
					sumQty,
					sumBasePrice,
					sumDisc,
					sumDiscPrice,
					sumCharge,
					avgQty,
					avgBasePrice,
					avgDisc,
					count));
		}
	}

	/**
	 * <pre>
	 *        l_returnflag, // string
	 *        l_linestatus, // string
	 *        sum(l_quantity) as sum_qty, // Long
	 *        sum(l_extendedprice) as sum_base_price, // Double
	 *        sum(l_discount) as sum_disc // Double
	 *        sum(l_extendedprice * (1-l_discount)) as sum_disc_price, // Double
	 *        sum(l_extendedprice * (1-l_discount) * (1+l_tax)) as sum_charge, // Double
	 *        avg(l_quantity) as avg_qty, // long
	 *        avg(l_extendedprice) as avg_price, // Double
	 *        avg(l_discount) as avg_disc, // Double
	 *        count(*) as count_order // long
	 * </pre>
	 */
	private static class Tuple11ToLineItemResult implements MapFunction<Tuple2<String, Tuple11<String, String, Long, Double, Double, Double, Double, Long, Double, Double, Long>>, String> {
		private static final long serialVersionUID = 1L;

		@Override
		public String map(Tuple2<String, Tuple11<String, String, Long, Double, Double, Double, Double, Long, Double, Double, Long>> value) throws Exception {
			String msg = "flag:" + value.f1.f0 +
				" status:" + value.f1.f1 +
				" sum_qty:" + value.f1.f2 +
				" sum_base_price:" + value.f1.f3 +
				" sum_disc:" + value.f1.f4 +
				" sum_disc_price:" + value.f1.f5 +
				" sum_charge:" + value.f1.f6 +
				" avg_qty:" + value.f1.f7 +
				" avg_price:" + value.f1.f8 +
				" avg_disc:" + value.f1.f9 +
				" order_qty:" + value.f1.f10;
			return msg;
		}
	}
}
