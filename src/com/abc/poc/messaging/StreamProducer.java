package com.abc.poc.messaging;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.abc.poc.messaging.vo.TaskEvent;
import com.lmax.disruptor.RingBuffer;
/**
 * 
 * @author Manoj Bansal
 * This is the event producer class. 
 *
 */
public class StreamProducer  {
	
	private static final Logger LOGGER = LoggerFactory.getLogger(StreamProducer.class);

	private RingBuffer<TaskEvent> ringBuffer;
	
	public StreamProducer(RingBuffer<TaskEvent> ringBuffer) {
		this.ringBuffer = ringBuffer;
	}
	
	public void produceEvents(int eventsPerProducer, String[] producersIds)  {
		LOGGER.info("Total no of producers : {} and evnets per producer are {}", producersIds.length, eventsPerProducer);
		for (String id : producersIds) {
			new Thread(
					() -> {
						for (int i = 1 ; i <= eventsPerProducer ; i++) {
							long seq = ringBuffer.next();
							TaskEvent taskEvent = ringBuffer.get(seq);
							taskEvent.setValue(id+""+i);
							ringBuffer.publish(seq);
						}
					}
			).start();
		}
	}
}
