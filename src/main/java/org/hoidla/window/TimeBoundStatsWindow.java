/*
 * hoidla: various time series algorithms for Big Data solutions
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

import org.chombo.stats.SimpleStat;
import org.hoidla.util.TimeStamped;
import org.hoidla.util.TimeStampedData;

/**
 * Stats with time bound window
 * @author pranab
 *
 */
public class TimeBoundStatsWindow extends TimeBoundWindow {
	private double mean;
	private double stdDev;
	private double min;
	private double max;
	private double median;
	private SimpleStat stats = new SimpleStat();

	public TimeBoundStatsWindow(long timeSpan, long timeStep, long processingTimeStep) {
		super(timeSpan, timeStep, processingTimeStep);
	}

	public TimeBoundStatsWindow(long timeSpan, long timeStep) {
		super(timeSpan, timeStep);
	}

	public TimeBoundStatsWindow(long timeSpan) {
		super(timeSpan);
	}

	/* (non-Javadoc)
	 * @see org.hoidla.window.DataWindow#processFullWindow()
	 */
	public void processFullWindow() {
		for (TimeStamped val : dataWindow) {
			TimeStampedData<Double> tsData = (TimeStampedData<Double>)val;
			double dValue = tsData.getValue();
			stats.add(dValue);
		}
		mean = stats.getMean();
		stdDev = stats.getStdDev();
		min = stats.getMin();
		max = stats.getMax();
		median = stats.getMedian();
		stats.initialize();
	}
	
	/**
	 * @return
	 */
	public double getMean() {
		return mean;
	}

	/**
	 * @return
	 */
	public double getStdDev() {
		return stdDev;
	}

	/**
	 * @return
	 */
	public double getMin() {
		return min;
	}

	/**
	 * @return
	 */
	public double getMax() {
		return max;
	}
	/**
	 * @return
	 */
	public double getMedian() {
		return median;
	}
	
}
