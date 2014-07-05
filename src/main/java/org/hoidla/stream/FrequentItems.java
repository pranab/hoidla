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

import org.hoidla.util.Expirer;

/**
 * Probabilistic frequent count algorithms
 * @author pranab
 *
 */
public class FrequentItems {

	public static class MisraGries {
		private Map<String, Integer> buckets = new HashMap<String, Integer>(); 
		private int maxBucket;
		private List<String> toBeRemoved = new ArrayList<String>(); 
		private Map<String, List<Long>> timestampedBuckets = new HashMap<String, List<Long>>(); 
		private Expirer expirer;

		/**
		 * @param maxBucket
		 */
		public MisraGries(int maxBucket ) {
			this.maxBucket = maxBucket;
		}
		
		public void setExpirer(Expirer expirer) {
			this.expirer = expirer;
		}

		/**
		 * add item
		 * @param value
		 */
		public void add(String value) {
			Integer count = buckets.get(value);
			if (null != count) {
				//existing
				buckets.put(value, count + 1);
			} else if (buckets.size() <  maxBucket) {
				//add to buckets
				buckets.put(value, 1);
			} else {
				//decrement each count
				toBeRemoved.clear();
				for (String key : buckets.keySet()) {
					int newCount = buckets.get(key) - 1;
					if (newCount > 0) {
						buckets.put(key, newCount);
					} else {
						toBeRemoved.add(key);
					}
				}
				for (String key : toBeRemoved) {
					buckets.remove(key);
				}
			}
		}

		/**
		 * add with time window based expiry
		 * @param value
		 */
		public void add(String value, long timestamp) {
			//expire old
			if (null != expirer) {
				for (String key : timestampedBuckets.keySet()) {
					List<Long> tsList = timestampedBuckets.get(key);
					expirer.expire(tsList, timestamp);
				}				
			}
			
			//add
			List<Long> timetsamps = timestampedBuckets.get(value);
			if (null != timetsamps) {
				//existing
				timetsamps.add(timestamp);
			} else if (timestampedBuckets.size() <  maxBucket) {
				//add to buckets
				timestampedBuckets.put(value, new ArrayList<Long>());
			} else {
				//remove oldest in each bucket
				toBeRemoved.clear();
				for (String key : timestampedBuckets.keySet()) {
					List<Long> tsList = timestampedBuckets.get(key);
					tsList.remove(0);
					if (tsList.isEmpty()) { 
						toBeRemoved.add(key);
					}
				}
				for (String key : toBeRemoved) {
					buckets.remove(key);
				}
			}
		}
		
		/**
		 * @return
		 */
		public Map<String, Integer> get() {
			return buckets;
		}
		
	}
}
