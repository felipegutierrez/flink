package org.apache.flink.streaming.examples.aggregate.udfs;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.flink.streaming.examples.aggregate.util.DataRateListener;
import org.apache.flink.streaming.examples.aggregate.util.TaxiRide;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.zip.GZIPInputStream;

public class TaxiRideSourceParallel extends RichParallelSourceFunction<TaxiRide> {

	private final int maxDelayMsecs;
	private final int watermarkDelayMSecs;

	private final String dataFilePath;
	private final int servingSpeed;
	private DataRateListener dataRateListener;
	private boolean running;
	private transient BufferedReader reader;
	private transient InputStream gzipStream;

	/**
	 * Serves the TaxiRide records from the specified and ordered gzipped input file.
	 * Rides are served exactly in order of their time stamps
	 * at the speed at which they were originally generated.
	 *
	 * @param dataFilePath The gzipped input file from which the TaxiRide records are read.
	 */
	public TaxiRideSourceParallel(String dataFilePath) {
		this(dataFilePath, 0, 1);
	}

	/**
	 * Serves the TaxiRide records from the specified and ordered gzipped input file.
	 * Rides are served exactly in order of their time stamps
	 * in a serving speed which is proportional to the specified serving speed factor.
	 *
	 * @param dataFilePath The gzipped input file from which the TaxiRide records are read.
	 * @param servingSpeedFactor The serving speed factor by which the logical serving time is adjusted.
	 */
	public TaxiRideSourceParallel(String dataFilePath, int servingSpeedFactor) {
		this(dataFilePath, 0, servingSpeedFactor);
	}

	/**
	 * Serves the TaxiRide records from the specified and ordered gzipped input file.
	 * Rides are served out-of time stamp order with specified maximum random delay
	 * in a serving speed which is proportional to the specified serving speed factor.
	 *
	 * @param dataFilePath The gzipped input file from which the TaxiRide records are read.
	 * @param maxEventDelaySecs The max time in seconds by which events are delayed.
	 * @param servingSpeedFactor The serving speed factor by which the logical serving time is adjusted.
	 */
	public TaxiRideSourceParallel(
		String dataFilePath,
		int maxEventDelaySecs,
		int servingSpeedFactor) {
		if (maxEventDelaySecs < 0) {
			throw new IllegalArgumentException("Max event delay must be positive");
		}
		this.running = true;
		this.dataFilePath = dataFilePath;
		this.maxDelayMsecs = maxEventDelaySecs * 1000;
		this.watermarkDelayMSecs = maxDelayMsecs < 10000 ? 10000 : maxDelayMsecs;
		this.servingSpeed = servingSpeedFactor;
	}

	@Override
	public void open(Configuration parameters) throws Exception {
		super.open(parameters);
		this.dataRateListener = new DataRateListener();
		this.dataRateListener.start();
	}

	@Override
	public void run(SourceContext<TaxiRide> sourceContext) throws Exception {

		while (running) {
			generateTaxiRideArray(sourceContext);
		}

		this.reader.close();
		this.reader = null;
		this.gzipStream.close();
		this.gzipStream = null;
	}

	private void generateTaxiRideArray(SourceContext<TaxiRide> sourceContext) throws Exception {
		gzipStream = new GZIPInputStream(new FileInputStream(dataFilePath));
		reader = new BufferedReader(new InputStreamReader(gzipStream, StandardCharsets.UTF_8));
		String line;
		TaxiRide taxiRide;
		long startTime;
		while (reader.ready() && (line = reader.readLine()) != null) {
			startTime = System.nanoTime();
			taxiRide = TaxiRide.fromString(line);

			sourceContext.collectWithTimestamp(taxiRide, getEventTime(taxiRide));

			// sleep in nanoseconds to have a reproducible data rate for the data source
			this.dataRateListener.busySleep(startTime);
		}
	}

	public long getEventTime(TaxiRide ride) {
		return ride.getEventTime();
	}

	@Override
	public void cancel() {
		try {
			this.running = false;
			if (this.reader != null) {
				this.reader.close();
			}
			if (this.gzipStream != null) {
				this.gzipStream.close();
			}
		} catch (IOException ioe) {
			throw new RuntimeException("Could not cancel SourceFunction", ioe);
		} finally {
			this.reader = null;
			this.gzipStream = null;
		}
	}
}
