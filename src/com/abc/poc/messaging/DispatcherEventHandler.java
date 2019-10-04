package com.abc.poc.messaging;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.abc.poc.messaging.vo.TaskEvent;
import com.lmax.disruptor.EventHandler;
import com.lmax.disruptor.RingBuffer;

/**
 * 
 * @author Manoj Bansal
 * 
 * This event handler will check the id of the producer and redirect the TaskEvent to either abDisruptor or cdDisruptor.
 *
 */
public class DispatcherEventHandler implements EventHandler<TaskEvent> {
	
	private static final Logger LOGGER = LoggerFactory.getLogger(DispatcherEventHandler.class);

	private RingBuffer<TaskEvent> abRingBuffer;
	
	private RingBuffer<TaskEvent> cdRingBuffer;
	
	public DispatcherEventHandler(RingBuffer<TaskEvent> abRingBuffer, RingBuffer<TaskEvent> cdRingBuffer) {
		this.abRingBuffer = abRingBuffer;
		this.cdRingBuffer = cdRingBuffer;
	}
	
	@Override
	public void onEvent(TaskEvent taskEvent, long arg1, boolean arg2) throws Exception {
		if(taskEvent.getValue().startsWith("A") || taskEvent.getValue().startsWith("B")) {
			LOGGER.debug("Redirecting task event {} to abDisruptor for processing.", taskEvent);
			long seq = abRingBuffer.next();
			TaskEvent abTaskEvent = abRingBuffer.get(seq);
			abTaskEvent.setValue(taskEvent.getValue());
			abRingBuffer.publish(seq);
		}
		if(taskEvent.getValue().startsWith("C") || taskEvent.getValue().startsWith("D")) {
			LOGGER.debug("Redirecting task event {} to cdDisruptor for processing.", taskEvent);
			long seq = cdRingBuffer.next();
			TaskEvent cdTaskEvent = cdRingBuffer.get(seq);
			cdTaskEvent.setValue(taskEvent.getValue());
			cdRingBuffer.publish(seq);
		}
		
	}
	

}
