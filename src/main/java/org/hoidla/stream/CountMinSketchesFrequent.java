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

import java.util.List;
import java.util.TreeMap;

import org.hoidla.util.BoundedSortedObjects;
import org.hoidla.util.Expirer;

/**
 * Frequent count by Count Min Sketch algorithm
 * @author pranab
 *
 */
public class CountMinSketchesFrequent  extends FrequentItems.FrequentItemsFinder {
	protected CountMinSketch minSketches;
	protected TreeMap<Integer, Object> orderedItems = new TreeMap<Integer, Object>();
	protected int mostFrequentCount;
	private BoundedSortedObjects sortedObjects;		
	private int freqCountLimitPercent;
	
	/**
	 * @param errorLimit
	 * @param errorProbLimit
	 * @param mostFrequentCount
	 */
	public CountMinSketchesFrequent(double errorLimit, double errorProbLimit, int mostFrequentCount,
			int freqCountLimitPercent) {
		minSketches = new CountMinSketch(errorLimit,  errorProbLimit);
		this.mostFrequentCount = mostFrequentCount;
		sortedObjects  = new  BoundedSortedObjects(mostFrequentCount);
		this.freqCountLimitPercent =  freqCountLimitPercent;
	}
	
	public CountMinSketchesFrequent(double errorLimit, double errorProbLimit, int mostFrequentCount,
			int freqCountLimitPercent, Expirer expirer) {
		minSketches = new CountMinSketch(errorLimit,  errorProbLimit, expirer);
		this.mostFrequentCount = mostFrequentCount;
		sortedObjects  = new  BoundedSortedObjects(mostFrequentCount);
		this.freqCountLimitPercent =  freqCountLimitPercent;
	}

	/*
	/**
	 * @param expirer
	 */
	public void setExpirer(Expirer expirer) {
		this.expirer = expirer;
	}

	/* (non-Javadoc)
	 * @see org.hoidla.stream.FrequentItems.FrequentItemsFinder#add(java.lang.Object)
	 */
	@Override
	public void add(Object value) {
		++count;
		minSketches.add(value);
		trackCount(value);
	}

	/* (non-Javadoc)
	 * @see org.hoidla.stream.FrequentItems.FrequentItemsFinder#add(java.lang.Object, long)
	 */
	@Override
	public void add(Object value, long sequence) {
		++count;
		minSketches.add(value, sequence);
		trackCount(value);
	}

	/**
	 * track top counts 
	 * @param value
	 */
	private void trackCount(Object item) {
		int itemCount = minSketches.getDistr(item);
		if ( itemCount > (count * freqCountLimitPercent) / 100) {
			sortedObjects.add(item, itemCount);
		}
	}

	/* (non-Javadoc)
	 * @see org.hoidla.stream.FrequentItems.FrequentItemsFinder#get()
	 */
	@Override
	public List<BoundedSortedObjects.SortableObject> get() {
		sortedObjects.truncate();
		return sortedObjects.get();	
	}
}
