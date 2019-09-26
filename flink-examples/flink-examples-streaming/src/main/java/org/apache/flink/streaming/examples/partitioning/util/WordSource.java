/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.flink.streaming.examples.partitioning.util;

import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.util.Date;

public class WordSource implements SourceFunction<String> {
	private static final long DEFAULT_INTERVAL_CHANGE_DATA_SOURCE = Time.minutes(1).toMilliseconds();
	private volatile boolean running = true;
	private String wordSourceType;
	private long times;

	public WordSource(String wordSourceType) {
		this(wordSourceType, Long.MAX_VALUE);
	}

	public WordSource(String wordSourceType, long times) {
		this.wordSourceType = wordSourceType;
		this.times = times;
	}

	@Override
	public void run(SourceContext<String> ctx) throws Exception {
		long count = 0;
		while (running && count < times) {
			final String[] words;
			if (WordSourceType.WORDS_SKEW.equals(wordSourceType)) {
				words = WordCountPartitionData.WORDS_SKEW;
			} else if (WordSourceType.FEW_WORDS.equals(wordSourceType)) {
				words = WordCountPartitionData.FEW_WORDS;
			} else if (WordSourceType.WORDS.equals(wordSourceType)) {
				words = WordCountPartitionData.WORDS;
			} else {
				words = WordCountPartitionData.WORDS;
			}
			for (int i = 0; i < words.length; i++) {
				String word = words[i];
				ctx.collectWithTimestamp(word, (new Date()).getTime());
			}
			Thread.sleep(1000);
			count++;
		}
		cancel();
	}

	@Override
	public void cancel() {
		running = false;
	}
}
