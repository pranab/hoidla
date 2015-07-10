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

package org.hoidla.window;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * Analyzes locality of event
 * @author pranab
 *
 */
public class SizeBoundEventLocalityAnalyzer extends SizeBoundWindow<Boolean> {
	private int minOccurence = 1;
	private long maxIntervalAverage = -1;
	private long maxIntervalMax = -1;
	private double score = 0;
	
	/**
	 * @param maxSize
	 * @param minOccurence
	 * @param maxIntervalAverage
	 * @param maxIntervalMax
	 */
	public SizeBoundEventLocalityAnalyzer(int maxSize, int minOccurence,
			long maxIntervalAverage, long maxIntervalMax) {
		super(maxSize);
		this.minOccurence = minOccurence;
		this.maxIntervalAverage = maxIntervalAverage;
		this.maxIntervalMax = maxIntervalMax;
	}
	
	/**
	 * @param maxSize
	 * @param minOccurence
	 */
	public SizeBoundEventLocalityAnalyzer(int maxSize, int minOccurence) {
		super(maxSize);
		this.minOccurence = minOccurence;
	}
	
	/**
	 * @param maxIntervalAverage
	 * @return
	 */
	public SizeBoundEventLocalityAnalyzer withMaxIntervalAverage(long maxIntervalAverage) {
		this.maxIntervalAverage = maxIntervalAverage;
		return this;
	}
	
	/**
	 * @param maxIntervalMax
	 * @return
	 */
	public SizeBoundEventLocalityAnalyzer withMaxIntervalMax(long maxIntervalMax) {
		this.maxIntervalMax = maxIntervalMax;
		return this;
	}

	/* (non-Javadoc)
	 * @see org.hoidla.window.DataWindow#processFullWindow()
	 */
	@Override
	public  void processFullWindow() {
		Iterator<Boolean> iter = this.getIterator();
		long i = 0;
		
		//window positions for event occurences
		List<Long> eventWindowPositions = new ArrayList<Long>();
		while (iter.hasNext()) {
			Boolean val = iter.next();
			if (val) {
				eventWindowPositions.add(i);
				++i;
			}
		}
		score = EventLoality.getScore(eventWindowPositions, minOccurence, maxIntervalAverage, maxIntervalMax, maxSize);
	}

	/**
	 * @return
	 */
	public double getScore() {
		return score;
	}
	
	
}