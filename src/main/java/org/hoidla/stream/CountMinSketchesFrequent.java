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
import java.util.TreeMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.hoidla.util.BoundedSortedObjects.SortableObject;
import org.hoidla.util.EpochObjectCounter;
import org.hoidla.util.Expirer;
import org.hoidla.util.Hashing;
import org.hoidla.util.ObjectCounter;
import org.hoidla.util.SequencedObjectCounter;
import org.hoidla.util.SimpleObjectCounter;

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
	private boolean globalTotalCount = false;
	private static final Logger LOG = LoggerFactory.getLogger(CountMinSketchesFrequent.class);

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
	
	/**
	 * @param errorLimit
	 * @param errorProbLimit
	 * @param mostFrequentCount
	 * @param freqCountLimitPercent
	 * @param expirer
	 */
	public CountMinSketchesFrequent(double errorLimit, double errorProbLimit, int mostFrequentCount,
			int freqCountLimitPercent, Expirer expirer) {
		minSketches = new CountMinSketch(errorLimit,  errorProbLimit, expirer);
		this.mostFrequentCount = mostFrequentCount;
		sortedObjects  = new  BoundedSortedObjects(mostFrequentCount);
		this.freqCountLimitPercent =  freqCountLimitPercent;
	}
	
	/**
	 * @param globalTotalCount true when item domain is narrow and false otherwise
	 * @return
	 */
	public CountMinSketchesFrequent withGlobalTotalCount(boolean globalTotalCount) {
		this.globalTotalCount = globalTotalCount;
		return this;
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
		int totalCount = 0;
		if (globalTotalCount) {
			//for narrow domain
			totalCount = minSketches.getCount();
		} else {
			//when domain is very large
			for(SortableObject obj : sortedObjects.get()){
				totalCount += minSketches.getDistr(obj.getItem());
			}
		}
		if ( itemCount > (totalCount * freqCountLimitPercent) / 100) {
			sortedObjects.add(item, itemCount);
		}
	}
	
	/**
	 * 
	 */
	public void refreshCount() {
		Map<Object, Integer> freqCounts = new HashMap<Object, Integer>();
		List<BoundedSortedObjects.SortableObject> topHitters = sortedObjects.get();
		for (BoundedSortedObjects.SortableObject topHitter : topHitters) {
			int count = minSketches.getDistr(topHitter.getItem());
			freqCounts.put(topHitter.getItem(), count);
		}
		
		sortedObjects.clear();
		for (Object item :  freqCounts.keySet()) {
			sortedObjects.add(item, freqCounts.get(item));
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
	
	public void expire() {
		minSketches.expire();
	}
	
	public void intialize() {
		minSketches.initialize();
	}
}
