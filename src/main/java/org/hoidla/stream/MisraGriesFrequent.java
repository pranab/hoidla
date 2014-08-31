/*
 * hoidla: various algorithms for Big Data solutions
 * Author: Pranab Ghosh
 * 
 * Licensed under the Apache License, Version 2.0 (the "License"); you
 * may not use this file except in compliance with the License. You may
 * obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0 
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied. See the License for the specific language governing
 * permissions and limitations under the License.
 */


package org.hoidla.stream;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.hoidla.util.BoundedSortedObjects;
import org.hoidla.util.Expirer;
import org.hoidla.util.ObjectCounter;
import org.hoidla.util.SequencedObjectCounter;
import org.hoidla.util.SimpleObjectCounter;

/**
 * @author pranab
 *
 * @param <T>
 */
public class MisraGriesFrequent  extends FrequentItems.FrequentItemsFinder {
	private Map<Object, ObjectCounter> buckets = new HashMap<Object, ObjectCounter>(); 
	private int maxBucket;
	
	/**
	 * @param maxBucket
	 */
	public MisraGriesFrequent(int maxBucket ) {
		this.maxBucket = maxBucket;
	}
	
	public MisraGriesFrequent(int maxBucket, Expirer expirer ) {
		this.maxBucket = maxBucket;
		this.expirer = expirer;
	}

	public void setExpirer(Expirer expirer) {
		this.expirer = expirer;
	}

	/**
	 * add item
	 * @param value
	 */
	public void add(Object value) {
		ObjectCounter counter = buckets.get(value);
		if (null != counter) {
			//existing bucket
			counter.increment();
		} else if (buckets.size() <  maxBucket) {
			//add new bucket
			counter = new SimpleObjectCounter();
			counter.increment();
			buckets.put(value, counter);
		} else {
			//decrement each count
			decrementAll();		
		}
	}

	/**
	 * add with  window based expiry
	 * @param value
	 */
	public void add(Object value, long timestamp) {
		//expire old 
		ObjectCounter counter = null;
		if (null != expirer) {
			toBeRemoved.clear();
			for (Object key : buckets.keySet()) {
				counter = buckets.get(key);
				counter.expire(expirer, timestamp);
				
				if (counter.isZero()) {
					toBeRemoved.add(key);
				}
			}				
			for (Object item :  toBeRemoved) {
				buckets.remove(item);
			}
		}
		
		//add
		counter = buckets.get(value);
		if (null != counter) {
			//existing bucket
			counter.increment();
		} else if (buckets.size() <  maxBucket) {
			//add new  bucket
			counter = new SequencedObjectCounter();
			counter.increment();
			buckets.put(value, counter);
		} else {
			//remove oldest in each bucket
			decrementAll();		
		}
	}

	/**
	 * 
	 */
	private void decrementAll() {
		ObjectCounter counter = null;
		toBeRemoved.clear();
		for (Object key : buckets.keySet()) {
			counter = buckets.get(key);
			counter.decrement();
			if (counter.isZero()) {
				toBeRemoved.add(key);
			}
		}
		for (Object key : toBeRemoved) {
			buckets.remove(key);
		}
	}
	
	/**
	 * 
	 * @return items ordered by count
	 */
	public List<BoundedSortedObjects.SortableObject> get() {
		BoundedSortedObjects sortedObjects  = new  BoundedSortedObjects(maxBucket);		
		for (Object key : buckets.keySet()) {
			sortedObjects.add(key, buckets.get(key).getCount());
		}		
		sortedObjects.truncate();
		return sortedObjects.get();
	}

	@Override
	public void expire() {
		// TODO Auto-generated method stub
	}
	
}
