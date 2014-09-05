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
import java.util.List;

import org.hoidla.util.BoundedSortedObjects;
import org.hoidla.util.Expirer;

/**
 * Probabilistic frequent count algorithms
 * @author pranab
 *
 */
public class FrequentItems {
	
	/**
	 * Context data for creating appropriate frequent counter object
	 * @author pranab
	 *
	 */
	public static class Context {
		public  String strategy;
		public int maxBucket;  
		public long expireWindow;
		public double errorLimit;
		public double errorProbLimit; 
		public int mostFrequentCount;
		public int freqCountLimitPercent;
	}
	
	/**
	 * factory method 
	 * @param strategy
	 * @param maxBucket
	 * @param expireWindow
	 * @return
	 */
	public static FrequentItemsFinder create(FrequentItems.Context context) {
		FrequentItemsFinder freqFinder = null;
		if (context.strategy.equals("MisraGries")) {
			freqFinder = new MisraGriesFrequent(context.maxBucket);
			if (context.expireWindow > 0) {
				freqFinder.setExpirer(new Expirer(context.expireWindow));
			}
		} else if (context.strategy.equals("CountMinSketches")) {
			if (context.expireWindow > 0) {
				freqFinder = new CountMinSketchesFrequent(context.errorLimit, context.errorProbLimit, 
						context.mostFrequentCount, context.freqCountLimitPercent, new Expirer(context.expireWindow));
			} else {
				freqFinder = new CountMinSketchesFrequent(context.errorLimit, context.errorProbLimit, 
						context.mostFrequentCount, context.freqCountLimitPercent);
			}
		} else {
			throw new IllegalArgumentException("unsupported frequent item algorithm");
		}
		return freqFinder;
	}


	/**
	 * @author pranab
	 *
	 * @param <T>
	 */
	public static abstract class  FrequentItemsFinder {
		protected long count;
		protected Expirer expirer;
		protected List<Object> toBeRemoved = new ArrayList<Object>(); 
		
		/**
		 * @param expirer
		 */
		public abstract void setExpirer(Expirer expirer);
		
		/**
		 * @param value
		 */
		public abstract void add(Object value);
		
		/**
		 * @param value
		 * @param timestamp
		 */
		public abstract void add(Object value, long timestamp);
		
		/**
		 * @return
		 */
		public abstract  List<BoundedSortedObjects.SortableObject> get();
		
		/**
		 * 
		 */
		public abstract  void expire();
	}
	
	/**
	 * interface for frequency distribution
	 * @author pranab
	 *
	 */
	public static interface FrequencyDistribution {
		
		/**
		 * @param value
		 */
		public void add(Object value);
		
		/**
		 * @param value
		 * @param sequence
		 */
		public void add(Object value, long sequence);
		
		/**
		 * @param value
		 * @return
		 */
		public int getDistr(Object value);
		
		/**
		 * 
		 */
		public void expire();
		
		/**
		 * 
		 */
		public void initialize();
		
		/**
		 * @return
		 */
		public int getCount();
		
	}
	
}
