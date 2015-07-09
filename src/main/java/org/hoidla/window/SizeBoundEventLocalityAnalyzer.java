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
	private int maxIntervalAverage = -1;
	private int maxIntervalMax = -1;
	private double score = 0;
	
	public SizeBoundEventLocalityAnalyzer(int maxSize, int minOccurence,
			int maxIntervalAverage, int maxIntervalMax) {
		super(maxSize);
		this.minOccurence = minOccurence;
		this.maxIntervalAverage = maxIntervalAverage;
		this.maxIntervalMax = maxIntervalMax;
	}
	
	public SizeBoundEventLocalityAnalyzer(int maxSize, int minOccurence) {
		super(maxSize);
		this.minOccurence = minOccurence;
	}
	
	public SizeBoundEventLocalityAnalyzer withMaxIntervalAverage(int maxIntervalAverage) {
		this.maxIntervalAverage = maxIntervalAverage;
		return this;
	}
	
	public SizeBoundEventLocalityAnalyzer withMaxIntervalMax(int maxIntervalMax) {
		this.maxIntervalMax = maxIntervalMax;
		return this;
	}

	@Override
	public  void processFullWindow() {
		Iterator<Boolean> iter = this.getIterator();
		int i = 0;
		
		//window positions for event occurences
		List<Integer> windowPositions = new ArrayList<Integer>();
		while (iter.hasNext()) {
			Boolean val = iter.next();
			if (val) {
				windowPositions.add(i);
				++i;
			}
		}
		
		boolean scoreSet = false;
		//check for min occurences
		if (windowPositions.size() < minOccurence) {
			score = 0;
			scoreSet = true;
		} 
		
		//check average interval if limit set
		if (!scoreSet && maxIntervalAverage > 0) {
			double avInterval = 0;
			for (int j = 0; j < windowPositions.size() - 1; ++j) 	{
				avInterval += (double)(windowPositions.get(j+1) - windowPositions.get(j));
			}
			avInterval /= (windowPositions.size() - 1);
			if (avInterval > maxIntervalAverage) {
				score = 0;
				scoreSet = true;
			}
		}
		
		//check max interval if limit set
		if (!scoreSet && maxIntervalMax > 0) {
			int maxInterval = 0;
			for (int j = 0; j < windowPositions.size() - 1; ++j) 	{
				int interval = windowPositions.get(j+1) - windowPositions.get(j);
				if (interval > maxInterval) {
					maxInterval = interval;
				}
			}
			if (maxInterval > maxIntervalMax) {
				score = 0;
				scoreSet = true;
			}
		}
		
		if (!scoreSet) {
			score = (double)windowPositions.size() / this.maxSize;
		}
	}

	public double getScore() {
		return score;
	}
	
	
}
