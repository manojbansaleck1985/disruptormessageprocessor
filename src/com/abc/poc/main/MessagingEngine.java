package com.abc.poc.main;

import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.abc.poc.messaging.CombinerEventHandler;
import com.abc.poc.messaging.ConsumerEventHandler;
import com.abc.poc.messaging.DispatcherEventHandler;
import com.abc.poc.messaging.StreamProducer;
import com.abc.poc.messaging.vo.TaskEvent;
import com.abc.poc.messaging.vo.TaskEventFactory;
import com.lmax.disruptor.BusySpinWaitStrategy;
import com.lmax.disruptor.EventHandler;
import com.lmax.disruptor.RingBuffer;
import com.lmax.disruptor.dsl.Disruptor;
import com.lmax.disruptor.dsl.ProducerType;
import com.lmax.disruptor.util.DaemonThreadFactory;

public class MessagingEngine {
	
	private static final Logger LOGGER  = LoggerFactory.getLogger(MessagingEngine.class);
	
	/**
	 * THis map is used for unit testing.
	 * Here in this map, system will store processed value and the index position in final queue (finalResultSetQueue below) 
	 */
	private Map<String, Integer> outputValueVsIndexPosition = new LinkedHashMap<String, Integer>();
	
	public void start(String[] producersId, int bufferSize, int totalEvents) {

		int eventsPerProducer = totalEvents/producersId.length;

		TaskEventFactory factory = new TaskEventFactory();
		
		/**
		 * Dispatcher distruptor 
		 * This disruptor will consume messages from multiple producer and publish to 
		 * 1. abConsumerDisruptor
		 * 2. cdConsumerDisruptor
		 */
		Disruptor<TaskEvent> dispatcherDisruptor = new Disruptor<TaskEvent>(factory, bufferSize,	DaemonThreadFactory.INSTANCE , ProducerType.MULTI, new BusySpinWaitStrategy());
		/**
		 * finalCombinerDisruptor 
		 * This disruptor will consume processed messages received from 
		 * 1. abConsumerDisruptor
		 * 2. cdConsumerDisruptor
		 */
		Disruptor<TaskEvent> finalCombinerDisruptor = new Disruptor<TaskEvent>(factory, bufferSize,	DaemonThreadFactory.INSTANCE , ProducerType.SINGLE, new BusySpinWaitStrategy());
		/**
		 * abConsumerDisruptor and  cdConsumerDisruptor
		 * These disruptors will consume messages from dispatcherDisruptor and  after processing publish the taskevent to finalCombinerDisruptor
		 */
		Disruptor<TaskEvent> abConsumerDisruptor = new Disruptor<TaskEvent>(factory, bufferSize, DaemonThreadFactory.INSTANCE , ProducerType.SINGLE, 	new BusySpinWaitStrategy());
		Disruptor<TaskEvent> cdConsumerDisruptor = new Disruptor<TaskEvent>(factory, bufferSize, DaemonThreadFactory.INSTANCE , ProducerType.SINGLE, 	new BusySpinWaitStrategy());

		
		EventHandler<TaskEvent> finalCombinerEventHandler = new CombinerEventHandler(outputValueVsIndexPosition, eventsPerProducer*producersId.length);
		RingBuffer<TaskEvent> finalCombinerRingBuffer = getRingBufferForDisruptor(finalCombinerDisruptor,	finalCombinerEventHandler);

		
		EventHandler<TaskEvent> abConsumerEventHandler = new ConsumerEventHandler(finalCombinerRingBuffer);
		RingBuffer<TaskEvent> abConsumerRingBuffer = getRingBufferForDisruptor(abConsumerDisruptor,	abConsumerEventHandler);

		
		EventHandler<TaskEvent> cdConsumerEventHandler = new ConsumerEventHandler(finalCombinerRingBuffer);
		RingBuffer<TaskEvent> cdConsumerRingBuffer = getRingBufferForDisruptor(cdConsumerDisruptor,	cdConsumerEventHandler);

		
		EventHandler<TaskEvent> dispatcherEventHandler = new DispatcherEventHandler(abConsumerRingBuffer, cdConsumerRingBuffer);
		RingBuffer<TaskEvent> dispatcherRingBuffer = getRingBufferForDisruptor(dispatcherDisruptor,	dispatcherEventHandler);

		LOGGER.info("Producing events for processing.");
		StreamProducer producer = new StreamProducer(dispatcherRingBuffer);
		producer.produceEvents(eventsPerProducer, producersId);
		
		LOGGER.info("Shutting down all disruptors.");
		dispatcherDisruptor.shutdown();
		cdConsumerDisruptor.shutdown();
		abConsumerDisruptor.shutdown();
		finalCombinerDisruptor.shutdown();
	}
	
	private RingBuffer<TaskEvent> getRingBufferForDisruptor(Disruptor<TaskEvent> disruptor,	EventHandler<TaskEvent> eventHandler) {
		disruptor.handleEventsWith(eventHandler);
		disruptor.start();
		RingBuffer<TaskEvent> ringBuffer = disruptor.getRingBuffer();
		return ringBuffer;
	}

	public Map<String, Integer> getOutputValueVsIndexPosition() {
		return outputValueVsIndexPosition;
	}
}
