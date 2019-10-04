package com.abc.poc.main;

import java.util.Map;

import org.junit.Assert;
import org.junit.Test;

public class MessagingEngineTest {

	/**
	 * This test will check whether sequence of message produced by individual producer 
	 * and sequence of the message received by final joiner queue is same or not.
	 * @throws Exception
	 */
	@Test
	public void test1() throws Exception {
		MessagingEngine engine = new MessagingEngine();
		String[] producersId = new String[]{"A","B","C","D"};
		engine.start(producersId,32, 18);
		Map<String, Integer> outputValueVsIndexPosition = engine.getOutputValueVsIndexPosition();

		if(outputValueVsIndexPosition.containsKey("A2 Processed"))
			Assert.assertTrue( outputValueVsIndexPosition.get("A1 Processed") < outputValueVsIndexPosition.get("A2 Processed"));
		if(outputValueVsIndexPosition.containsKey("A3 Processed"))
			Assert.assertTrue( outputValueVsIndexPosition.get("A2 Processed") < outputValueVsIndexPosition.get("A3 Processed"));
		if(outputValueVsIndexPosition.containsKey("A4 Processed"))
			Assert.assertTrue( outputValueVsIndexPosition.get("A3 Processed") < outputValueVsIndexPosition.get("A4 Processed"));

		if(outputValueVsIndexPosition.containsKey("B2 Processed"))
			Assert.assertTrue( outputValueVsIndexPosition.get("B1 Processed") < outputValueVsIndexPosition.get("B2 Processed"));
		if(outputValueVsIndexPosition.containsKey("B3 Processed"))
			Assert.assertTrue( outputValueVsIndexPosition.get("B2 Processed") < outputValueVsIndexPosition.get("B3 Processed"));
		if(outputValueVsIndexPosition.containsKey("B4 Processed"))
			Assert.assertTrue( outputValueVsIndexPosition.get("B3 Processed") < outputValueVsIndexPosition.get("B4 Processed"));

		if(outputValueVsIndexPosition.containsKey("C2 Processed"))
			Assert.assertTrue( outputValueVsIndexPosition.get("C1 Processed") < outputValueVsIndexPosition.get("C2 Processed"));
		if(outputValueVsIndexPosition.containsKey("C3 Processed"))
			Assert.assertTrue( outputValueVsIndexPosition.get("C2 Processed") < outputValueVsIndexPosition.get("C3 Processed"));
		if(outputValueVsIndexPosition.containsKey("C4 Processed"))
			Assert.assertTrue( outputValueVsIndexPosition.get("C3 Processed") < outputValueVsIndexPosition.get("C4 Processed"));

		if(outputValueVsIndexPosition.containsKey("D2 Processed"))
			Assert.assertTrue( outputValueVsIndexPosition.get("D1 Processed") < outputValueVsIndexPosition.get("D2 Processed"));
		if(outputValueVsIndexPosition.containsKey("D3 Processed"))
			Assert.assertTrue( outputValueVsIndexPosition.get("D2 Processed") < outputValueVsIndexPosition.get("D3 Processed"));
		if(outputValueVsIndexPosition.containsKey("D4 Processed"))
			Assert.assertTrue( outputValueVsIndexPosition.get("D3 Processed") < outputValueVsIndexPosition.get("D4 Processed"));
	}

}
