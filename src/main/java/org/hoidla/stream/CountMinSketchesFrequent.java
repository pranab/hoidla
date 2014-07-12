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

public class CountMinSketchesFrequent  implements FrequentItems.FrequentItemsFinder {
	protected Expirer expirer;
	protected CountMinSketch minSketches;
	protected TreeMap<Integer, Object> orderedItems = new TreeMap<Integer, Object>();
	protected int mostFrequentCount;

	/**
	 * @param errorLimit
	 * @param errorProbLimit
	 * @param mostFrequentCount
	 */
	public CountMinSketchesFrequent(double errorLimit, double errorProbLimit, int mostFrequentCount) {
		minSketches = new CountMinSketch(errorLimit,  errorProbLimit);
		this.mostFrequentCount = mostFrequentCount;
	}
	
	public CountMinSketchesFrequent(double errorLimit, double errorProbLimit, int mostFrequentCount,
			Expirer expirer) {
		minSketches = new CountMinSketch(errorLimit,  errorProbLimit, expirer);
		this.mostFrequentCount = mostFrequentCount;
	}

	/*
	/**
	 * @param expirer
	 */
	public void setExpirer(Expirer expirer) {
		this.expirer = expirer;
	}

	public void add(Object value) {
		minSketches.add(value);
		trackCount(value);
	}

	public void add(Object value, long sequence) {
		minSketches.add(value, sequence);
		trackCount(value);
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

	@Override
	public Map<Integer, Object> get() {
		TreeMap<Integer, Object> orderedItems = new TreeMap<Integer, Object>();
		for (Integer key :  this.orderedItems.keySet()) {
			orderedItems.put(key, this.orderedItems.get(key));
		}
		return orderedItems;
	}
	
}
