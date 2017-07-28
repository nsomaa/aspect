package com.aspect.interview;

import java.util.Arrays;
import java.util.Iterator;
import java.util.PriorityQueue;
import java.util.Queue;
import java.util.concurrent.locks.ReentrantLock;

import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.Seconds;
import org.springframework.stereotype.Service;

/**
 * Work item object.
 *
 */
class WorkItem implements Comparable<WorkItem> {
	public enum Class {
		NORMAL,
		PRIORITY,
		VIP,
		MANAGEMENT_OVERRIDE
	}
	
	/**
	 * Work item id
	 */
	private long id;
	/**
	 * Work item class
	 */
	private Class itemClass;
	/**
	 * Time (UTC) when the work item is added to queue
	 */
	private DateTime timeAdded;
	
	/**
	 * Returns work item class based on the id.
	 * @param id
	 * @return work item class
	 */
	public static Class getItemClass(long id) {
		Class itemClass;
		// determine item class
		if(id % 3 == 0 && id % 5 == 0) {
			itemClass = Class.MANAGEMENT_OVERRIDE;
		}
		else if(id % 3 == 0) {
			itemClass = Class.PRIORITY;
		}
		else if(id % 5 == 0) {
			itemClass = Class.VIP;
		}
		else {
			itemClass = Class.NORMAL;
		}
		
		return itemClass;
	}
	
	/**
	 * Constructor
	 * @param id work item id
	 * @param timeAdded time when the item was added to queue 
	 */
	public WorkItem(long id, DateTime timeAdded) {
		this.id = id;
		this.timeAdded = timeAdded;
		this.itemClass = WorkItem.getItemClass(id);
	}
	
	/**
	 * Returns work item id
	 * @return work item id
	 */
	public long getId() {
		return this.id;
	}
	
	/**
	 * Returns time in seconds how long the work item is in queue.
	 * @return waited time in seconds
	 */
	public int waitedTime() {
		DateTime now = new DateTime(DateTimeZone.UTC);
		int seconds =  Seconds.secondsBetween(this.timeAdded, now).getSeconds();
		return seconds;
	}
	
	/** Returns wait time since the specified time.
	 * @return waited time in seconds
	 */
	public int waitedTime(DateTime time) {
		int seconds =  Seconds.secondsBetween(this.timeAdded, time).getSeconds();
		if(seconds < 0) {
			seconds  = 0;
		}
		return seconds;
	}
	
	/** 
	 * Returns work item's classification
	 * @return work item's classification
	 */
	public Class getItemClass() {
		return this.itemClass;
	}

	/**
	 * Computes priority rank based on item classification.
	 * 
	 * @return priority rank
	 */
	private double computeRank() {
		double rank = 0.0;
		double secondsElapsed = (double)this.waitedTime();
		
		if(itemClass == Class.MANAGEMENT_OVERRIDE ||
		   itemClass == Class.NORMAL) {
			rank = secondsElapsed;
		}
		else if(itemClass == Class.PRIORITY) {
			rank = Math.max(3.0,secondsElapsed*Math.log(secondsElapsed));
		}
		else {
			// VIP queue
			rank = Math.max(4.0,2.0*secondsElapsed*Math.log(secondsElapsed));
		}
		System.out.println(String.format("id: %d, rank: %f", this.id, rank));
		return rank;
	}
	
	/**
	 * Returns seconds elapsed since the specified time
	 * @param time  time
	 * @return seconds elapsed
	 */
	public int secondsElapsedSince(DateTime time) {
		return Seconds.secondsBetween(time, this.timeAdded).getSeconds();
	}
	
	/**
	 * Compares a work item with the current work item object using the priority rank.
	 */
	public int compareTo(WorkItem o) {
		int ret = Double.compare(o.computeRank(),
							  this.computeRank());
		return ret;
	}
	
	
	/**
	 * Returns true if work items are equal. Only uses work item id for comparison.
	 */
	public boolean equals(Object o) {
		if(o == this) {
			return true;
		}
		
		if(o == null || o.getClass() != this.getClass()) {
			return false;
		}
		WorkItem obj = (WorkItem)o;
		return obj.id == this.id;	
	}
}

/**
 * Priority Queue service provides queuing work items. The priority is determined
 * by the work item classification.
 * 
 */
@Service
public class AspectPriorityQueue {
	
    /**
     * Management override work items are added to a separate queue, since they are 
     * ahead of all the other types of work items.
     */	
	private Queue<WorkItem> managementOverrideQueue;
    
	/**
	 * Non-mamagement override work items such as, Normal, Priority, VIP work items
	 * are stored in the general priority queue. The priority is determined by a formula for 
	 * thes types of work items.
     */
	private Queue<WorkItem> generalQueue;
	
	/**
	 * A lock to protect adding to and removing from the general queue. 
	 */
	private ReentrantLock lockGeneralQueue;

	/**
	 * A lock to protect adding to and removing from the management override queue. 
	 */
	private ReentrantLock lockManagementQueue;
	
	public AspectPriorityQueue() {
		managementOverrideQueue = new PriorityQueue<WorkItem>();
		generalQueue = new PriorityQueue<WorkItem>();
		lockGeneralQueue = new ReentrantLock();
		lockManagementQueue = new ReentrantLock();
	}

    /**
     * Queues workitem in the General queue (non-management override work items)
     *
     * @param workItem work item object
     * @throws ItemAlreadyExistsException if the work item already exists in the queue
     */
	private void enqueueGeneral(WorkItem workItem) throws ItemAlreadyExistsException {
		final ReentrantLock lock = this.lockGeneralQueue;
		lock.lock();
		try {
			if(generalQueue.contains(workItem)) {
				throw new ItemAlreadyExistsException("Object already exists");
			}
			generalQueue.offer(workItem);
		}
		finally {
			lock.unlock();
		}
	}

    /**
     * Queues workitem in the managment-override queue
     *
     * @param workItem work item object
     * @throws ItemAlreadyExistsException if the workitem already exists in the queue
     */
	private void enqueueManagement(WorkItem workItem) throws ItemAlreadyExistsException {
		final ReentrantLock lock = this.lockManagementQueue;
		lock.lock();
		try {
			if(managementOverrideQueue.contains(workItem)) {
				throw new ItemAlreadyExistsException("Object already exists");
			}
			managementOverrideQueue.offer(workItem);
		}
		finally {
			lock.unlock();
		}
	}
	
    /**
     * Removes the top work item from the general queue
     *
     * @return returns the work item from the queue. If the queue is empty, returns null.
     */
	private WorkItem dequeueGeneral() {
		final ReentrantLock lock = this.lockGeneralQueue;
		lock.lock();
		try {
			return generalQueue.poll();
		}
		finally {
			lock.unlock();
		}
	}

    /**
     * Removes the top work item from the management override queue
     *
     * @return returns the work item from the queue. If the queue is empty, returns null.
     */
	private WorkItem dequeueManagement() {
		final ReentrantLock lock = this.lockManagementQueue;
		lock.lock();
		try {
			return managementOverrideQueue.poll();
		}
		finally {
			lock.unlock();
		}
	}
	
    /**
     * Queues work item into priority queue.
     *
     * @param id work item id
     * @param timeAdded UTC date time when the item was added to the queue  
     * 
     * @throws IllegalArgumentException if the date/time is later than current time
     * @throws ItemAlreadyExistsException if the id is already present in the queue
     */
	public void enqueue(long id, DateTime timeAdded) throws IllegalArgumentException, ItemAlreadyExistsException {
		DateTime now = new DateTime(DateTimeZone.UTC);
		if(timeAdded.compareTo(now) > 0) {
			throw new IllegalArgumentException("Queueing date is later than current time");
		}
		WorkItem workItem = new WorkItem(id, timeAdded);
		if(workItem.getItemClass() == WorkItem.Class.MANAGEMENT_OVERRIDE) {
			enqueueManagement(workItem);
		}
		else {
			enqueueGeneral(workItem);
		}
	}
	
    /**
     * Removes the top work item from the priority queue.
     *
     * @return  top work item. If no work items is available, then returns null.
     */
	public WorkItem dequeue() {
		WorkItem workItem = dequeueManagement();
		if (workItem == null) {
			workItem = dequeueGeneral();
		}
		return workItem;
	}
	
    /**
     * Returns current position in the queue of the work item.
     *
     * @return  queue position. -1 if the work item is not in the queue.
     */
	public int getPosition(long id) {
		final ReentrantLock lockManagement = this.lockManagementQueue;
		final ReentrantLock lockGeneral = this.lockGeneralQueue;
		lockManagement.lock();
		lockGeneral.lock();
		int index = -1;
		try {
			if(!managementOverrideQueue.isEmpty()) {
				WorkItem[] array = new WorkItem[managementOverrideQueue.size()];
				Arrays.sort(managementOverrideQueue.toArray(array));
				for(int i = 0; i < array.length; i++) {
					if(array[i].getId() == id) {
						index =  i;
						break;
					}
				}
			}
			
			if(index == -1 && !generalQueue.isEmpty()) {
				WorkItem[] array = new WorkItem[generalQueue.size()];
				Arrays.sort(generalQueue.toArray(array));
				for(int i = 0; i < array.length; i++) {
					if(array[i].getId() == id) {
						index = i + managementOverrideQueue.size();
						break;
					}
				}
			}			
		}
		finally {
			lockManagement.unlock();
			lockGeneral.unlock();
		}
		return index;
	}
	
    /**
     * Removes the specified work item.
     *
     * @return  true if the item was found and removed. Otherwise, false.
     */	
	public boolean remove(long id) {
		boolean removed = false;
		
		// create a dummy object and try to remove it
		// remove calls WorkItem.equals() find the object in the queue.
		WorkItem dummy = new WorkItem(id, new DateTime());

		final ReentrantLock lockManagement = this.lockManagementQueue;
		try {
			removed = managementOverrideQueue.remove(dummy);
		}
		finally {
			lockManagement.unlock();
		}
		if(!removed) {
			final ReentrantLock lock = this.lockGeneralQueue;
			lock.lock();
			
			try {
				removed = generalQueue.remove(dummy);
			}
			finally {
				lock.unlock();
			}
		}
		
		return removed;
	}

    /**
     * Returns work item ids.
     *
     * @return  work item ids.
     */
	public long[] getIds() {
		final ReentrantLock lockManagement = this.lockManagementQueue;
		final ReentrantLock lockGeneral = this.lockGeneralQueue;
		lockManagement.lock();
		lockGeneral.lock();
		long[] ret = null;
		try {
			int totalItems = managementOverrideQueue.size() + generalQueue.size();
			ret = new long[totalItems];
			
			if(!managementOverrideQueue.isEmpty()) {
				WorkItem[] array = new WorkItem[managementOverrideQueue.size()];
				Arrays.sort(managementOverrideQueue.toArray(array));
				for(int i = 0; i < array.length; i++) {
					ret[i] = array[i].getId();
				}
			}
			
			if(!generalQueue.isEmpty()) {
				WorkItem[] array = new WorkItem[generalQueue.size()];
				Arrays.sort(generalQueue.toArray(array));
				for(int i = 0; i < array.length; i++) {
					ret[i+managementOverrideQueue.size()] = array[i].getId();
				}
			}			
		}
		finally {
			lockManagement.unlock();
			lockGeneral.unlock();
		}
		return ret;
	}
	
    /**
     * Returns average wait time in the queue since the specified time.
     *
     * @return  average wait time
     */	
	public double getAverageWaitTime(DateTime currentTime) {
		final ReentrantLock lockManagement = this.lockManagementQueue;
		final ReentrantLock lockGeneral = this.lockGeneralQueue;
		lockManagement.lock();
		lockGeneral.lock();
		try {
			long numberOfItems = managementOverrideQueue.size() + generalQueue.size();
			long totalWaitedTime = 0;
			
			if(!managementOverrideQueue.isEmpty()) {
				Iterator<WorkItem> it = managementOverrideQueue.iterator();
				while(it.hasNext()) {
					totalWaitedTime += it.next().waitedTime(currentTime);
				}
			}
			
			if(!generalQueue.isEmpty()) {
				Iterator<WorkItem> it = generalQueue.iterator();
				while(it.hasNext()) {
					totalWaitedTime += it.next().waitedTime(currentTime);
				}
			}
			return (double)totalWaitedTime / (double)numberOfItems;
		}
		finally {
			lockManagement.unlock();
			lockGeneral.unlock();
		}
	}
	
	/**
	 * Empties the queue.
	 */
	public void clear() {
		managementOverrideQueue.clear();
		generalQueue.clear();
	}

}
