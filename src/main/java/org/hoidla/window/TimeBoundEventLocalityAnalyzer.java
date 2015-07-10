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

import org.hoidla.util.TimeStamped;
import org.hoidla.util.TimeStampedFlag;

/**
 * @author pranab
 *
 */
public class TimeBoundEventLocalityAnalyzer extends TimeBoundWindow {
	private int minOccurence = 1;
	private long maxIntervalAverage = -1;
	private long maxIntervalMax = -1;
	private double score;

	private static final long serialVersionUID = -7873039731214593449L;

	/**
	 * @param timeSpan
	 * @param minOccurence
	 */
	public TimeBoundEventLocalityAnalyzer(long timeSpan, int minOccurence) {
		super(timeSpan);
		this.minOccurence = minOccurence;
	}
	
	/**
	 * @param timeSpan
	 * @param timeStep
	 * @param minOccurence
	 */
	public TimeBoundEventLocalityAnalyzer(long timeSpan, long timeStep, int minOccurence) {
		super(timeSpan, timeStep);
		this.minOccurence = minOccurence;
	}
	
	/**
	 * @param timeSpan
	 * @param timeStep
	 * @param processingTimeStep
	 * @param minOccurence
	 */
	public TimeBoundEventLocalityAnalyzer(long timeSpan, long timeStep, long processingTimeStep, int minOccurence) {
		super(timeSpan, timeStep, processingTimeStep);
		this.minOccurence = minOccurence;
	}
	
	/**
	 * @param maxIntervalAverage
	 * @return
	 */
	public TimeBoundEventLocalityAnalyzer withMaxIntervalAverage(long maxIntervalAverage) {
		this.maxIntervalAverage = maxIntervalAverage;
		return this;
	}
	
	/**
	 * @param maxIntervalMax
	 * @return
	 */
	public TimeBoundEventLocalityAnalyzer withMaxIntervalMax(long maxIntervalMax) {
		this.maxIntervalMax = maxIntervalMax;
		return this;
	}
	
	@Override
	public  void processFullWindow() {
		Iterator<TimeStamped> iter = this.getIterator();
		
		//window positions for event occurences
		List<Long> eventWindowPositions = new ArrayList<Long>();
		while (iter.hasNext()) {
			TimeStampedFlag val = (TimeStampedFlag)iter.next();
			if (val.getFlag()) {
				eventWindowPositions.add(val.getTimeStamp());
			}
		}
		
		score = EventLoality.getScore(eventWindowPositions, minOccurence, maxIntervalAverage, maxIntervalMax, size());
	}
	
	/**
	 * @return
	 */
	public double getScore() {
		return score;
	}
	
}
