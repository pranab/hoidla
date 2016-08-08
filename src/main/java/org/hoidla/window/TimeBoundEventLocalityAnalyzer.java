/*
 * hoidla: various streaming algorithms for Big Data solutions
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

import org.hoidla.util.ExplicitlyTimeStampedFlag;
import org.hoidla.util.TimeStamped;

/**
 * @author pranab
 *
 */
public class TimeBoundEventLocalityAnalyzer extends TimeBoundWindow {
	private double score = -1.0;
	private EventLocality.Context context;

	private static final long serialVersionUID = -7873039731214593449L;

	/**
	 * @param timeSpan
	 * @param minOccurence
	 */
	public TimeBoundEventLocalityAnalyzer(long timeSpan, long timeStep, EventLocality.Context context) {
		super(timeSpan, timeStep);
		this.context = context;
	}

	@Override
	public  void processFullWindow() {
		Iterator<TimeStamped> iter = this.getIterator();
		
		//window positions for event occurences
		List<Long> eventWindowPositions = new ArrayList<Long>();
		while (iter.hasNext()) {
			ExplicitlyTimeStampedFlag val = (ExplicitlyTimeStampedFlag)iter.next();
			if (val.getFlag()) {
				eventWindowPositions.add(val.getTimeStamp());
			}
		}

		if (null != context.singleStatregies) {
			score = EventLocality.getTimedEventSingleScore(eventWindowPositions, context.minOccurence, context.maxIntervalAverage, 
				context.maxIntervalMax, context.singleStatregies, size(), context.anyCond);
		} else {
			score =   EventLocality.getTimedEventWeightedScore(eventWindowPositions, context.minOccurence, context.maxIntervalAverage, 
					context.maxIntervalMax, context.aggregateWeightedStrategies,  size());
		}
	}
	
	/**
	 * @return
	 */
	public double getScore() {
		return score;
	}
	
}
