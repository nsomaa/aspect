package com.aspect.interview;

import static org.junit.Assert.*;

import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormatter;
import org.joda.time.format.ISODateTimeFormat;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;
import org.springframework.test.context.support.AnnotationConfigContextLoader;

import com.aspect.interview.AspectPriorityQueue;
import com.aspect.interview.ItemAlreadyExistsException;

@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(loader = AnnotationConfigContextLoader.class, classes = {AppConfig.class})
public class QueueTestSuite {
	@Autowired
	AspectPriorityQueue	queue;
	
	@BeforeClass
	public static void setUpBeforeClass() throws Exception {
	}

	@AfterClass
	public static void tearDownAfterClass() throws Exception {
	}

	@Before
	public void setUp() throws Exception {
	}

	@After
	public void tearDown() throws Exception {
		 queue.clear();
	}

	@Test
	public void testNormalOp() throws IllegalArgumentException, ItemAlreadyExistsException {
		 DateTimeFormatter parser    = ISODateTimeFormat.dateTimeNoMillis();
		 
		 // queue items (time is in UTC)
		 DateTime time = parser.parseDateTime("2017-07-28T12:00:00+00:00");
		 queue.enqueue(5, time);
		 queue.enqueue(3, time);
		 time = parser.parseDateTime("2017-07-28T12:00:01+00:00");
		 queue.enqueue(2, time);
		 queue.enqueue(15, time);
		 
		 // test getIds() call
		 // priority position should be [15, 5, 3, 2]
		 long[] items = queue.getIds();
		 assertEquals(4, items.length);
		 assertArrayEquals(new long[] {15, 5, 3, 2}, items);
		 
		 // test getPosition() call
		 long pos = queue.getPosition(15);
		 assertEquals(0, pos);
		 pos = queue.getPosition(5);
		 assertEquals(1, pos);
		 pos = queue.getPosition(3);
		 assertEquals(2, pos);
		 pos = queue.getPosition(2);
		 assertEquals(3, pos);
		 pos = queue.getPosition(25);
		 assertEquals(-1, pos);

		 // test dequeue operation
		 WorkItem item = queue.dequeue();
		 assertEquals(15, item.getId());
		 item = queue.dequeue();
		 assertEquals(item.getId(), 5);
		 item = queue.dequeue();
		 assertEquals(item.getId(), 3);
		 item = queue.dequeue();
		 assertEquals(item.getId(), 2);
	}
	
	@Test
	public void testWithSameTime() throws IllegalArgumentException, ItemAlreadyExistsException {
		 DateTimeFormatter parser    = ISODateTimeFormat.dateTimeNoMillis();
		 DateTime time = parser.parseDateTime("2017-07-28T12:00:00+00:00");
		 queue.enqueue(2, time);
		 queue.enqueue(3, time);
		 queue.enqueue(5, time);
		 queue.enqueue(15, time);
		 WorkItem item = queue.dequeue();
		 assertEquals(15, item.getId());
		 item = queue.dequeue();
		 assertEquals(item.getId(), 5);
		 item = queue.dequeue();
		 assertEquals(item.getId(), 3);
		 item = queue.dequeue();
		 assertEquals(item.getId(), 2);
	}	

	@Test
	public void testAvgTime() throws IllegalArgumentException, ItemAlreadyExistsException {
		 DateTimeFormatter parser    = ISODateTimeFormat.dateTimeNoMillis();
		 DateTime time = parser.parseDateTime("2017-07-28T12:00:00+00:00");
		 queue.enqueue(2, time);
		 time = parser.parseDateTime("2017-07-28T12:00:05+00:00");
		 queue.enqueue(3, time);
		 time = parser.parseDateTime("2017-07-28T12:00:10+00:00");
		 queue.enqueue(5, time);
		 time = parser.parseDateTime("2017-07-28T12:00:15+00:00");
		 queue.enqueue(15, time);
		 time = parser.parseDateTime("2017-07-28T12:00:20+00:00");
		 int avgWaitTime = (int)queue.getAverageWaitTime(time);
		 assertEquals(12,avgWaitTime);
	}
		
}
