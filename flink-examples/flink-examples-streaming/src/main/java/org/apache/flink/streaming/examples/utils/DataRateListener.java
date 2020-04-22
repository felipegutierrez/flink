package org.apache.flink.streaming.examples.utils;

import java.io.*;
import java.nio.charset.StandardCharsets;

public class DataRateListener extends Thread implements Serializable {

	public static final String DATA_RATE_FILE = "/home/flink/tmp/datarate.txt";
	private long delay;
	private boolean running;

	public DataRateListener() {
		this.delay = 1000;
		this.running = true;
		this.disclaimer();
	}

	private void disclaimer() {
		System.out.println(DataRateListener.class.getSimpleName() + " class to read data rate from file [" + DATA_RATE_FILE + "] in milliseconds.");
		System.out.println("This listener reads every 60 seconds only the first line from the data rate file.");
		System.out.println("Use the following command to change the millisecond data rate:");
		System.out.println("echo \"10\" > /home/flink/tmp/datarate.txt");
		System.out.println();
	}

	public void run() {
		while (running) {
			File fileName = new File(DATA_RATE_FILE);

			try (BufferedReader br = new BufferedReader(new InputStreamReader(
				new FileInputStream(fileName), StandardCharsets.UTF_8))) {

				String line;
				while ((line = br.readLine()) != null) {
					System.out.println(line);
					if (isInteger(line)) {
						System.out.println("Reading new frequency to generate Taxi data: " + line + " milliseconds.");
						delay = Integer.parseInt(line);
					} else if ("SHUTDOWN".equalsIgnoreCase(line)) {
						running = false;
					} else {
						System.out.println(line);
					}
				}
				Thread.sleep(60 * 1000);
			} catch (FileNotFoundException e) {
				e.printStackTrace();
			} catch (InterruptedException e) {
				e.printStackTrace();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}

	public long getDelay() {
		return delay;
	}

	private boolean isInteger(String s) {
		return isInteger(s, 10);
	}

	private boolean isInteger(String s, int radix) {
		if (s.isEmpty())
			return false;
		for (int i = 0; i < s.length(); i++) {
			if (i == 0 && s.charAt(i) == '-') {
				if (s.length() == 1)
					return false;
				else
					continue;
			}
			if (Character.digit(s.charAt(i), radix) < 0)
				return false;
		}
		return true;
	}
}
