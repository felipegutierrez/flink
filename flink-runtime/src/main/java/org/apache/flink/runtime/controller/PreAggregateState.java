package org.apache.flink.runtime.controller;

public class PreAggregateState {

	private final int subtaskIndex;
	// Network buffer usage
	private final long[] outPoolUsageMin;
	private final long[] outPoolUsageMax;
	private final double[] outPoolUsageMean;
	private final double[] outPoolUsage05;
	private final double[] outPoolUsage075;
	private final double[] outPoolUsage095;
	private final double[] outPoolUsage099;
	private final double[] outPoolUsageStdDev;

	// Latency
	private final long[] latencyMin;
	private final long[] latencyMax;
	private final double[] latencyMean;
	private final double[] latencyQuantile05;
	private final double[] latencyQuantile099;
	private final double[] latencyStdDev;

	// Throughput
	private final double[] numRecordsInPerSecond;
	private final double[] numRecordsOutPerSecond;

	// Pre-agg parameter K
	private final int[] minCount;

	public PreAggregateState(String subtaskIndex, String outPoolUsageMin, String outPoolUsageMax, String outPoolUsageMean,
							 String outPoolUsage05, String outPoolUsage075, String outPoolUsage095, String outPoolUsage099, String outPoolUsageStdDev,
							 String latencyMin, String latencyMax, String latencyMean, String latencyQuantile05, String latencyQuantile099, String latencyStdDev,
							 String numRecordsInPerSecond, String numRecordsOutPerSecond, String minCountCurrent) {

		this.subtaskIndex = Integer.parseInt(subtaskIndex);

		// Network buffer usage
		this.outPoolUsageMin = new long[]{Long.parseLong(outPoolUsageMin), -1, -1};
		this.outPoolUsageMax = new long[]{Long.parseLong(outPoolUsageMax), -1, -1};
		this.outPoolUsageMean = new double[]{Double.parseDouble(outPoolUsageMean), -1.0, -1.0};
		this.outPoolUsage05 = new double[]{Double.parseDouble(outPoolUsage05), -1.0, -1.0};
		this.outPoolUsage075 = new double[]{Double.parseDouble(outPoolUsage075), -1.0, -1.0};
		this.outPoolUsage095 = new double[]{Double.parseDouble(outPoolUsage095), -1.0, -1.0};
		this.outPoolUsage099 = new double[]{Double.parseDouble(outPoolUsage099), -1.0, -1.0};
		this.outPoolUsageStdDev = new double[]{Double.parseDouble(outPoolUsageStdDev), -1.0, -1.0};

		// Latency
		this.latencyMin = new long[]{Long.parseLong(latencyMin), -1, -1};
		this.latencyMax = new long[]{Long.parseLong(latencyMax), -1, -1};
		this.latencyMean = new double[]{Double.parseDouble(latencyMean), -1.0, -1.0};
		this.latencyQuantile05 = new double[]{Double.parseDouble(latencyQuantile05), -1.0, -1.0};
		this.latencyQuantile099 = new double[]{Double.parseDouble(latencyQuantile099), -1.0, -1.0};
		this.latencyStdDev = new double[]{Double.parseDouble(latencyStdDev), -1.0, -1.0};

		// Throughput
		this.numRecordsInPerSecond = new double[]{Double.parseDouble(numRecordsInPerSecond), -1.0, -1.0};
		this.numRecordsOutPerSecond = new double[]{Double.parseDouble(numRecordsOutPerSecond), -1.0, -1.0};

		// Pre-agg parameter K
		this.minCount = new int[]{Integer.parseInt(minCountCurrent), -1, -1};
	}

	public static void main(String[] args) {
		PreAggregateState p = new PreAggregateState("0", "1", "1", "1",
			"1", "1", "1", "1", "1",
			"1", "1", "1", "1", "1", "1",
			"1", "1", "1");
		p.update("0", "1", "1", "1",
			"1", "1", "1", "1", "1",
			"1", "1", "1", "1", "1", "1",
			"1", "1", "1");
		System.out.println("subtask[" + p.getSubtaskIndex() + "] OutPoolUsageMin[" + p.getOutPoolUsageMin() +
			"] OutPoolUsageMean[" + p.getOutPoolUsageMean() + "]");

		p.update("0", "1", "1", "1",
			"1", "1", "1", "1", "1",
			"1", "1", "1", "1", "1", "1",
			"1", "1", "1");
		p.update("0", "1", "1", "1",
			"1", "1", "1", "1", "1",
			"1", "1", "1", "1", "1", "1",
			"1", "1", "1");
		System.out.println("subtask[" + p.getSubtaskIndex() + "] OutPoolUsageMin[" + p.getOutPoolUsageMin() +
			"] OutPoolUsageMean[" + p.getOutPoolUsageMean() + "]");
	}

	public void update(String subtaskIndex, String outPoolUsageMin, String outPoolUsageMax, String outPoolUsageMean,
					   String outPoolUsage05, String outPoolUsage075, String outPoolUsage095, String outPoolUsage099, String outPoolUsageStdDev,
					   String latencyMin, String latencyMax, String latencyMean, String latencyQuantile05, String latencyQuantile099, String latencyStdDev,
					   String numRecordsInPerSecond, String numRecordsOutPerSecond, String minCountCurrent) {
		if (this.subtaskIndex != Integer.parseInt(subtaskIndex)) {
			System.out.println("ERROR: current subtaskIndex[" + subtaskIndex + "] is not equal to the state subtaskIndex[" + this.subtaskIndex + "]");
			return;
		}
		// Network buffer usage
		long outPoolUsageMin01 = this.outPoolUsageMin[0];
		long outPoolUsageMin02 = this.outPoolUsageMin[1];
		this.outPoolUsageMin[0] = Long.parseLong(outPoolUsageMin);
		this.outPoolUsageMin[1] = outPoolUsageMin01;
		this.outPoolUsageMin[2] = outPoolUsageMin02;

		long outPoolUsageMax01 = this.outPoolUsageMax[0];
		long outPoolUsageMax02 = this.outPoolUsageMax[1];
		this.outPoolUsageMax[0] = Long.parseLong(outPoolUsageMax);
		this.outPoolUsageMax[1] = outPoolUsageMax01;
		this.outPoolUsageMax[2] = outPoolUsageMax02;

		double outPoolUsageMean01 = this.outPoolUsageMean[0];
		double outPoolUsageMean02 = this.outPoolUsageMean[1];
		this.outPoolUsageMean[0] = Double.parseDouble(outPoolUsageMean);
		this.outPoolUsageMean[1] = outPoolUsageMean01;
		this.outPoolUsageMean[2] = outPoolUsageMean02;

		double outPoolUsage0501 = this.outPoolUsage05[0];
		double outPoolUsage0502 = this.outPoolUsage05[1];
		this.outPoolUsage05[0] = Double.parseDouble(outPoolUsage05);
		this.outPoolUsage05[1] = outPoolUsage0501;
		this.outPoolUsage05[2] = outPoolUsage0502;

		double outPoolUsage07501 = this.outPoolUsage075[0];
		double outPoolUsage07502 = this.outPoolUsage075[1];
		this.outPoolUsage075[0] = Double.parseDouble(outPoolUsage075);
		this.outPoolUsage075[1] = outPoolUsage07501;
		this.outPoolUsage075[2] = outPoolUsage07502;

		double outPoolUsage09501 = this.outPoolUsage095[0];
		double outPoolUsage09502 = this.outPoolUsage095[1];
		this.outPoolUsage095[0] = Double.parseDouble(outPoolUsage095);
		this.outPoolUsage095[1] = outPoolUsage09501;
		this.outPoolUsage095[2] = outPoolUsage09502;

		double outPoolUsage09901 = this.outPoolUsage099[0];
		double outPoolUsage09902 = this.outPoolUsage099[1];
		this.outPoolUsage099[0] = Double.parseDouble(outPoolUsage099);
		this.outPoolUsage099[1] = outPoolUsage09901;
		this.outPoolUsage099[2] = outPoolUsage09902;

		double outPoolUsageStdDev01 = this.outPoolUsageStdDev[0];
		double outPoolUsageStdDev02 = this.outPoolUsageStdDev[1];
		this.outPoolUsageStdDev[0] = Double.parseDouble(outPoolUsageStdDev);
		this.outPoolUsageStdDev[1] = outPoolUsageStdDev01;
		this.outPoolUsageStdDev[2] = outPoolUsageStdDev02;

		// Latency
		long latencyMin01 = this.latencyMin[0];
		long latencyMin02 = this.latencyMin[1];
		this.latencyMin[0] = Long.parseLong(latencyMin);
		this.latencyMin[1] = latencyMin01;
		this.latencyMin[2] = latencyMin02;

		long latencyMax01 = this.latencyMax[0];
		long latencyMax02 = this.latencyMax[1];
		this.latencyMax[0] = Long.parseLong(latencyMax);
		this.latencyMax[1] = latencyMax01;
		this.latencyMax[2] = latencyMax02;

		double latencyMean01 = this.latencyMean[0];
		double latencyMean02 = this.latencyMean[1];
		this.latencyMean[0] = Double.parseDouble(latencyMean);
		this.latencyMean[1] = latencyMean01;
		this.latencyMean[2] = latencyMean02;

		double latencyQuantile0501 = this.latencyQuantile05[0];
		double latencyQuantile0502 = this.latencyQuantile05[1];
		this.latencyQuantile05[0] = Double.parseDouble(latencyQuantile05);
		this.latencyQuantile05[1] = latencyQuantile0501;
		this.latencyQuantile05[2] = latencyQuantile0502;

		double latencyQuantile09901 = this.latencyQuantile099[0];
		double latencyQuantile09902 = this.latencyQuantile099[1];
		this.latencyQuantile099[0] = Double.parseDouble(latencyQuantile099);
		this.latencyQuantile099[1] = latencyQuantile09901;
		this.latencyQuantile099[2] = latencyQuantile09902;

		double latencyStdDev01 = this.latencyStdDev[0];
		double latencyStdDev02 = this.latencyStdDev[1];
		this.latencyStdDev[0] = Double.parseDouble(latencyStdDev);
		this.latencyStdDev[1] = latencyStdDev01;
		this.latencyStdDev[2] = latencyStdDev02;

		// Throughput
		double numRecordsInPerSecond01 = this.numRecordsInPerSecond[0];
		double numRecordsInPerSecond02 = this.numRecordsInPerSecond[1];
		this.numRecordsInPerSecond[0] = Double.parseDouble(numRecordsInPerSecond);
		this.numRecordsInPerSecond[1] = numRecordsInPerSecond01;
		this.numRecordsInPerSecond[2] = numRecordsInPerSecond02;

		double numRecordsOutPerSecond01 = this.numRecordsOutPerSecond[0];
		double numRecordsOutPerSecond02 = this.numRecordsOutPerSecond[1];
		this.numRecordsOutPerSecond[0] = Double.parseDouble(numRecordsOutPerSecond);
		this.numRecordsOutPerSecond[1] = numRecordsOutPerSecond01;
		this.numRecordsOutPerSecond[2] = numRecordsOutPerSecond02;

		// Pre-agg parameter K
		int minCount01 = this.minCount[0];
		int minCount02 = this.minCount[1];
		this.minCount[0] = Integer.parseInt(minCountCurrent);
		this.minCount[1] = minCount01;
		this.minCount[2] = minCount02;
	}

	public int getSubtaskIndex() {
		return subtaskIndex;
	}

	public int getMinCount() {
		return minCount[0];
	}

	public long getOutPoolUsageMin() {
		return average(outPoolUsageMin);
	}

	public long getOutPoolUsageMax() {
		return average(outPoolUsageMax);
	}

	public double getOutPoolUsageMean() {
		return average(outPoolUsageMean);
	}

	public double getOutPoolUsage05() {
		return average(outPoolUsage05);
	}

	public double getOutPoolUsage075() {
		return average(outPoolUsage075);
	}

	public double getOutPoolUsage095() {
		return average(outPoolUsage095);
	}

	public double getOutPoolUsage099() {
		return average(outPoolUsage099);
	}

	public double getOutPoolUsageStdDev() {
		return average(outPoolUsageStdDev);
	}

	public long getLatencyMin() {
		return average(latencyMin);
	}

	public long getLatencyMax() {
		return average(latencyMax);
	}

	public double getLatencyMean() {
		return average(latencyMean);
	}

	public double getLatencyQuantile05() {
		return average(latencyQuantile05);
	}

	public double getLatencyQuantile099() {
		return average(latencyQuantile099);
	}

	public double getLatencyStdDev() {
		return average(latencyStdDev);
	}

	public double getNumRecordsInPerSecond() {
		return average(numRecordsInPerSecond);
	}

	public double getNumRecordsOutPerSecond() {
		return average(numRecordsOutPerSecond);
	}

	private int average(int[] values) {
		int sum = 0;
		int count = 0;
		for (int i = 0; i < values.length; i++) {
			if (values[i] > 0) {
				sum = sum + values[i];
				count++;
			}
		}
		if (count == 0) {
			return 0;
		}
		return (int) Math.ceil(sum / count);
	}

	private long average(long[] values) {
		long sum = 0;
		int count = 0;
		for (int i = 0; i < values.length; i++) {
			if (values[i] > 0) {
				sum = sum + values[i];
				count++;
			}
		}
		if (count == 0) {
			return 0;
		}
		return (long) Math.ceil(sum / count);
	}

	private double average(double[] values) {
		double sum = 0;
		int count = 0;
		for (int i = 0; i < values.length; i++) {
			if (values[i] > 0) {
				sum = sum + values[i];
				count++;
			}
		}
		if (count == 0) {
			return 0;
		}
		return Math.ceil(sum / count);
	}
}
