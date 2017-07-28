package com.aspect.interview;


import javax.servlet.http.HttpServletResponse;

import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormatter;
import org.joda.time.format.ISODateTimeFormat;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class PriorityQueueController {
	@Autowired
	AspectPriorityQueue priorityQueueService;
	
	/**
	 * Returns queue position for given work item id.
	 * @param id work item id
	 * @return queue position. -1 if the id doesn't exist in the queue
	 */
	@RequestMapping("/pos/{id}")
	public long getPosition(@PathVariable("id") long id)  {
		long index = -1;
		if(id > 0) {
			index = priorityQueueService.getPosition(id);
		}
		return index;
	}
	
	/**
	 * Queues a work item.
	 * @param id work item id
	 * @param time time when the work item is added to queue
	 * @param response REST servlet response. 
	 * 				   HTTP status 409 if the item already exists in the queue
	 *        		   HTTP status 400 if the parameters are invalid.
	 */
	@RequestMapping(value="/enqueue/{id}/time/{time}", method = RequestMethod.PUT)
	@ResponseBody
	public void enqueue(@PathVariable("id") long id, @PathVariable("time") String time, HttpServletResponse response) {
		 try {
			// first validate id
			if(id <= 0 ) {
				// throw a REST exception
				throw new IllegalArgumentException("Invalid id");
			}
			
			if(id <= 0 || time.isEmpty()) {
				// throw a REST exception
				throw new IllegalArgumentException("Invalid arguments");
			}

			// time is expected to be in ISO8601 format
			if(time.length() == 19) {
				 // add utc timezone for parsing
				 time = time + "+00:00";
			 }
			DateTimeFormatter parser    = ISODateTimeFormat.dateTimeNoMillis().withZoneUTC();
			DateTime queuedTime = null;
			queuedTime = parser.parseDateTime(time);
			if(queuedTime == null) {
				throw new IllegalArgumentException("Invalid date/time");
			}
			
			// queue the request
			priorityQueueService.enqueue(id, queuedTime);
		 }
		 catch(ItemAlreadyExistsException e) {
			 // return 409
			 response.setStatus(HttpServletResponse.SC_CONFLICT);
		 }
		 catch(IllegalArgumentException e) {
			 response.setStatus(HttpServletResponse.SC_BAD_REQUEST);
		 }
	}
	
	/**
	 * Dequeues an item from the priority queue. If no item is available in the queue, returns 404.
	 * @param response HTTP servlet response
	 * @return None 
	 *         HTTP status 404 if no items available in the queue
	 */
	@RequestMapping("/dequeue")
	@ResponseBody
	public long dequeue(HttpServletResponse response)  {
		WorkItem item = priorityQueueService.dequeue();
		if(item == null) {
			// throw not found exception
			 response.setStatus(HttpServletResponse.SC_NOT_FOUND);
		}
		return item.getId();
	}
	
	/**
	 * Returns all the work item ids in the priority order.
	 * 
	 * @return work item ids in the priority order
	 */
	@RequestMapping("/items")
	@ResponseBody
	public long[] getItems() {
		long[] items = priorityQueueService.getIds();
		return items;
	}
	
	/**
	 * Returns average wait time in the queue.
	 * 
	 * @param time used for calculating wait time of each work item
	 * @param response servlet response
	 *                 HTTP Status 400 if the time is in invalid format
	 * @return average wait time
	 */
	@RequestMapping("/meanwaittime/{since}")
	@ResponseBody
	public int getMeanWaitTime(@PathVariable("since") String time, HttpServletResponse response) {
		 DateTimeFormatter parser    = ISODateTimeFormat.dateTimeNoMillis();
		 DateTime currentTime = null;
		 try {
			// time is in ISO8601 format
			if(time.isEmpty()) {
				throw new IllegalArgumentException("Invalid time specified");
			}
			 currentTime = parser.parseDateTime(time);
		 }
		 catch(IllegalArgumentException e) {
			 response.setStatus(HttpServletResponse.SC_BAD_REQUEST);
		 }

		return (int)priorityQueueService.getAverageWaitTime(currentTime);
	}
}
