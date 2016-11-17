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
	private double score = 0;
	private EventLocality.Context context;
	
	
	public SizeBoundEventLocalityAnalyzer(int maxSize, EventLocality.Context context) {
		super(maxSize);
		this.context = context;
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
		
		if (null != context.singleStatregies) {
			score = EventLocality.getPositionalEventSingleScore(eventWindowPositions, context.minOccurence, 
					context.maxIntervalAverage, false, 0, context.maxIntervalMax, context.minRangeLength,  
					context.singleStatregies, maxSize, context.anyCond);
		} else {
			score =   EventLocality.getPositionalWeightedScore(eventWindowPositions, context.minOccurence, 
					context.maxIntervalAverage, context.maxIntervalMax, context.minRangeLength, 
					context.aggregateWeightedStrategies, maxSize);
		}
	}

	/**
	 * @return
	 */
	public double getScore() {
		return score;
	}
	
	
}
