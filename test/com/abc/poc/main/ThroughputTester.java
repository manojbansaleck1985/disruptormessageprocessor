package com.abc.poc.main;

import java.io.IOException;

import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.runner.RunnerException;

public class ThroughputTester {

	public static void main(String[] args) throws RunnerException, IOException {
		org.openjdk.jmh.Main.main(args);
	}
	
	@Benchmark
	@Fork(value = 1, warmups = 1)
	@Measurement(iterations = 3)
	@Warmup(iterations = 3)
	@BenchmarkMode(Mode.Throughput)
	public void BenchmarkThroughput() throws NumberFormatException, Exception {
//		e.messagingEngine.start(Integer.valueOf(e.totalTasks), 10);
	}
}
