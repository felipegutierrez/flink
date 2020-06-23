package org.apache.flink.streaming.examples.aggregate.util;

import io.airlift.tpch.LineItem;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.apache.flink.streaming.examples.utils.DataRateListener;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.text.DateFormat;
import java.text.SimpleDateFormat;

public class LineItemSource extends RichSourceFunction<LineItem> {

	private static final transient DateFormat df = new SimpleDateFormat("yyyy-MM-dd");
	private final String dataFilePath;
	private DataRateListener dataRateListener;
	private boolean running;
	private transient BufferedReader reader;
	private transient InputStream stream;

	public LineItemSource(String dataFilePath) {
		this.running = true;
		this.dataFilePath = dataFilePath;
	}

	@Override
	public void open(Configuration parameters) throws Exception {
		super.open(parameters);
		this.dataRateListener = new DataRateListener();
		this.dataRateListener.start();
	}

	@Override
	public void run(SourceContext<LineItem> sourceContext) throws Exception {
		while (running) {
			generateLineItemArray(sourceContext);
		}
		this.reader.close();
		this.reader = null;
		this.stream.close();
		this.stream = null;
	}

	private void generateLineItemArray(SourceContext<LineItem> sourceContext) throws Exception {
		stream = new FileInputStream(dataFilePath);
		reader = new BufferedReader(new InputStreamReader(stream, StandardCharsets.UTF_8));
		String line;
		LineItem lineItem;
		int rowNumber = 0;
		long startTime;
		while (reader.ready() && (line = reader.readLine()) != null) {
			startTime = System.nanoTime();
			rowNumber++;
			lineItem = getLineItem(line, rowNumber);
			sourceContext.collectWithTimestamp(lineItem, getEventTime(lineItem));

			// sleep in nanoseconds to have a reproducible data rate for the data source
			this.dataRateListener.busySleep(startTime);
		}
	}

	private LineItem getLineItem(String line, int rowNumber) {
		String[] tokens = line.split("\\|");
		if (tokens.length != 16) {
			throw new RuntimeException("Invalid record: " + line);
		}
		LineItem lineItem;
		try {
			long orderKey = Long.parseLong(tokens[0]);
			long partKey = Long.parseLong(tokens[1]);
			long supplierKey = Long.parseLong(tokens[2]);
			int lineNumber = Integer.parseInt(tokens[3]);
			long quantity = Long.parseLong(tokens[4]);
			long extendedPrice = (long) Double.parseDouble(tokens[5]);
			long discount = (long) Double.parseDouble(tokens[6]);
			long tax = (long) Double.parseDouble(tokens[7]);
			String returnFlag = tokens[8];
			String status = tokens[9];
			int shipDate = (int) df.parse(tokens[10]).getTime();
			int commitDate = (int) df.parse(tokens[11]).getTime();
			int receiptDate = (int) df.parse(tokens[12]).getTime();
			String shipInstructions = tokens[13];
			String shipMode = tokens[14];
			String comment = tokens[15];

			lineItem = new LineItem(rowNumber, orderKey, partKey, supplierKey, lineNumber, quantity, extendedPrice, discount,
				tax, returnFlag, status, shipDate, commitDate, receiptDate, shipInstructions, shipMode, comment);
		} catch (NumberFormatException nfe) {
			throw new RuntimeException("Invalid record: " + line, nfe);
		} catch (Exception e) {
			throw new RuntimeException("Invalid record: " + line, e);
		}
		return lineItem;
	}

	public long getEventTime(LineItem ride) {
		// return ride.getEventTime();
		return 0;
	}

	@Override
	public void cancel() {
		try {
			this.running = false;
			if (this.reader != null) {
				this.reader.close();
			}
			if (this.stream != null) {
				this.stream.close();
			}
		} catch (IOException ioe) {
			throw new RuntimeException("Could not cancel SourceFunction", ioe);
		} finally {
			this.reader = null;
			this.stream = null;
		}
	}
}
