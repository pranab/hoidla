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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import org.hoidla.stream.FrequentItems.FrequentItemsFinder;
import org.hoidla.util.Expirer;
import org.hoidla.util.ObjectCounter;
import org.hoidla.util.SequencedObjectCounter;
import org.hoidla.util.SimpleObjectCounter;

/**
 * @author pranab
 *
 * @param <T>
 */
public class MisraGriesFrequent<T>  implements FrequentItems.FrequentItemsFinder<T> {
	private Map<T, ObjectCounter> buckets = new HashMap<T, ObjectCounter>(); 
	private int maxBucket;
	private List<T> toBeRemoved = new ArrayList<T>(); 
	private Expirer expirer;

	/**
	 * @param maxBucket
	 */
	public MisraGriesFrequent(int maxBucket ) {
		this.maxBucket = maxBucket;
	}
	
	public void setExpirer(Expirer expirer) {
		this.expirer = expirer;
	}

	/**
	 * add item
	 * @param value
	 */
	public void add(T value) {
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
	 * add with time window based expiry
	 * @param value
	 */
	public void add(T value, long timestamp) {
		//expire old 
		ObjectCounter counter = null;
		if (null != expirer) {
			for (T key : buckets.keySet()) {
				counter = buckets.get(key);
				counter.expire(expirer, timestamp);
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
		for (T key : buckets.keySet()) {
			counter = buckets.get(key);
			counter.decrement();
			if (counter.isZero()) {
				toBeRemoved.add(key);
			}
		}
		for (T key : toBeRemoved) {
			buckets.remove(key);
		}
	}
	
	/**
	 * 
	 * @return items ordered by count
	 */
	public Map<Integer, T> get() {
		TreeMap<Integer, T> orderItems = new TreeMap<Integer, T>();
		for (T key : buckets.keySet()) {
			orderItems.put(buckets.get(key).getCount(), key);
		}				
		return orderItems;
	}
	
}
