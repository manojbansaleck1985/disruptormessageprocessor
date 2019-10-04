package com.abc.poc.messaging;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.abc.poc.messaging.vo.TaskEvent;
import com.lmax.disruptor.EventHandler;
import com.lmax.disruptor.RingBuffer;

/**
 * 
 * @author Manoj Bansal
 * This class will consume the message and process it and send it to the combinerDisruptor.
 *
 */
public class ConsumerEventHandler implements EventHandler<TaskEvent> {
	
	private static final Logger LOGGER = LoggerFactory.getLogger(ConsumerEventHandler.class);
	
	private RingBuffer<TaskEvent> ringBuffer;

	public ConsumerEventHandler(RingBuffer<TaskEvent>  ringBuffer) {
		this.ringBuffer = ringBuffer;
	}
	
	@Override
	public void onEvent(TaskEvent taskEvent, long arg1, boolean arg2) throws Exception {
		
		long seq = ringBuffer.next();
		TaskEvent finalCombinerTaskEvent = ringBuffer.get(seq);
		taskEvent.setProcessedValue(String.join(" ", taskEvent.getValue(), "Processed"));
		LOGGER.debug("Processed value {}", taskEvent.getProcessedValue());
		finalCombinerTaskEvent.setValue(taskEvent.getValue());
		finalCombinerTaskEvent.setProcessedValue(taskEvent.getProcessedValue());
		ringBuffer.publish(seq);
	}
	

}
