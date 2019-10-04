package com.abc.poc.messaging.vo;

import com.lmax.disruptor.EventFactory;

public class TaskEventFactory implements EventFactory<TaskEvent> {

	@Override
	public TaskEvent newInstance() {
		return new TaskEvent();
	}

}
