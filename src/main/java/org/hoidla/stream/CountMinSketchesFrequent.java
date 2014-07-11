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

import java.util.Map;
import java.util.TreeMap;

import org.hoidla.stream.FrequentItems.FrequentItemsFinder;
import org.hoidla.util.Expirer;

public class CountMinSketchesFrequent {
	/**
	 * @author pranab
	 *
	 */
	public static class CountMinSketchesCounter  {
		protected Expirer expirer;
		protected FrequencyDistribution.CountMinSketch minSketches;
		protected TreeMap<Integer, Object> orderedItems = new TreeMap<Integer, Object>();
		protected int mostFrequentCount;
	
		/**
		 * @param errorLimit
		 * @param errorProbLimit
		 * @param mostFrequentCount
		 */
		public CountMinSketchesCounter(double errorLimit, double errorProbLimit, int mostFrequentCount) {
			minSketches = new FrequencyDistribution.CountMinSketch(errorLimit,  errorProbLimit);
			this.mostFrequentCount = mostFrequentCount;
		}
		
		/**
		 * @param expirer
		 */
		public void setExpirer(Expirer expirer) {
			this.expirer = expirer;
		}

		/**
		 * @param value
		 */
		protected void trackCount(Object value) {
			//get frequency count and store in tree map
			int count = minSketches.getDistr(value);
			orderedItems.put(count, value);
			if (orderedItems.size() > mostFrequentCount) {
				Integer smallestKey = orderedItems.firstKey();
				orderedItems.remove(smallestKey);
			}
		}
		
	}
	
	/**
	 * @author pranab
	 *
	 */
	public static class CountMinSketchesString  extends CountMinSketchesCounter  
		implements FrequentItemsFinder<String>{

		/**
		 * @param errorLimit
		 * @param errorProbLimit
		 * @param mostFrequentCount
		 */
		public CountMinSketchesString(double errorLimit, double errorProbLimit, int mostFrequentCount) {
			super(errorLimit, errorProbLimit, mostFrequentCount);
		}

		/* (non-Javadoc)
		 * @see org.hoidla.stream.FrequentItems.FrequentItemsFinder#add(java.lang.Object)
		 */
		@Override
		public void add(String value) {
			minSketches.add(value);
			trackCount(value);
		}
		
		/* (non-Javadoc)
		 * @see org.hoidla.stream.FrequentItems.FrequentItemsFinder#add(java.lang.Object, long)
		 */
		@Override
		public void add(String value, long timestamp) {
			// TODO Auto-generated method stub
			
		}

		/* (non-Javadoc)
		 * @see org.hoidla.stream.FrequentItems.FrequentItemsFinder#get()
		 */
		@Override
		public Map<Integer, String> get() {
			TreeMap<Integer, String> orderedItems = new TreeMap<Integer, String>();
			for (Integer key :  this.orderedItems.keySet()) {
				orderedItems.put(key, (String)this.orderedItems.get(key));
			}
			return orderedItems;
		}
		
	}

	/**
	 * @author pranab
	 *
	 */
	public static class CountMinSketchesInteger  extends CountMinSketchesCounter  
		implements FrequentItemsFinder<Integer>{

		/**
		 * @param errorLimit
		 * @param errorProbLimit
		 * @param mostFrequentCount
		 */
		public CountMinSketchesInteger(double errorLimit, double errorProbLimit, int mostFrequentCount) {
			super(errorLimit, errorProbLimit, mostFrequentCount);
		}

		/* (non-Javadoc)
		 * @see org.hoidla.stream.FrequentItems.FrequentItemsFinder#add(java.lang.Object)
		 */
		@Override
		public void add(Integer value) {
			minSketches.add(value);
			trackCount(value);
		}
		
		/* (non-Javadoc)
		 * @see org.hoidla.stream.FrequentItems.FrequentItemsFinder#add(java.lang.Object, long)
		 */
		@Override
		public void add(Integer value, long timestamp) {
			// TODO Auto-generated method stub
			
		}

		/* (non-Javadoc)
		 * @see org.hoidla.stream.FrequentItems.FrequentItemsFinder#get()
		 */
		@Override
		public Map<Integer, Integer> get() {
			TreeMap<Integer, Integer> orderedItems = new TreeMap<Integer, Integer>();
			for (Integer key :  this.orderedItems.keySet()) {
				orderedItems.put(key, (Integer)this.orderedItems.get(key));
			}
			return orderedItems;
		}
		
	}
	
}
