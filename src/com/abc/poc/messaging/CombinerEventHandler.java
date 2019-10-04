package com.abc.poc.messaging;

import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.abc.poc.messaging.vo.TaskEvent;
import com.lmax.disruptor.EventHandler;

public class CombinerEventHandler implements EventHandler<TaskEvent> {
	
	private static final Logger LOGGER = LoggerFactory.getLogger(CombinerEventHandler.class);

	/**
	 * Finally storing all task evens in one of the list which can be easily tested.
	 */
	private Map<String, Integer> outputValueVsIndexPosition;
	
	private int totalEvents;
	
	private AtomicInteger count = new AtomicInteger(1);
	
	public CombinerEventHandler(Map<String, Integer> outputValueVsIndexPosition, int totalEvents) {
		this.outputValueVsIndexPosition = outputValueVsIndexPosition;
		this.totalEvents = totalEvents;
	}

	@Override
	public void onEvent(TaskEvent taskEvent, long arg1, boolean arg2) throws Exception {
		if(count.get() < totalEvents) {
			outputValueVsIndexPosition.put(taskEvent.getProcessedValue(),count.getAndIncrement());
		}
		LOGGER.debug("Message : Value={} processedValue={}", taskEvent.getValue(), taskEvent.getProcessedValue());
	}

}
