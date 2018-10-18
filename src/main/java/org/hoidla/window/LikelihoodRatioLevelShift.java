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

import org.apache.commons.math3.stat.StatUtils;

/**
 * @author pranab
 *
 */
public class LikelihoodRatioLevelShift extends SizeBoundWindow<Double> {
	private int minSegmentSize;
	private double tStat;
	private double firstMean;
	private double secondMean;
	private int shiftPos;
	
	public LikelihoodRatioLevelShift(int maxSize) {
		super(maxSize);
	}
	
	public LikelihoodRatioLevelShift(int maxSize, int minSegmentSize) {
		super(maxSize);
		this.minSegmentSize = minSegmentSize;
	}

	public void processFullWindow() {
		double[] data = new double[maxSize];
		int i = 0;
		for (double d : dataWindow) {
			data[i++] = d;
		}
		double m1 = 0;
		double m2 = 0;
		double v1 = 0;
		double v2 = 0;
		
		tStat = 0;
		for (int s = minSegmentSize ; s < maxSize - minSegmentSize; ++s) {
			m1 = StatUtils.mean(data, 0, s);
			v1 = StatUtils.variance(data, m1, 0, s);
			m2 = StatUtils.mean(data, s, maxSize-s);
			v2 = StatUtils.variance(data, m2, s, maxSize-s);
			double t = (m1 - m2) / Math.sqrt((s * v1 +  (maxSize-s) * v2) / (s * (maxSize-s)));
			t = Math.abs(t);
			if (t > tStat) {
				tStat = t;
				firstMean = m1;
				secondMean = m2;
				shiftPos = s;
			} 
		}
	}

	public double gettStat() {
		return tStat;
	}

	public double getFirstMean() {
		return firstMean;
	}

	public double getSecondMean() {
		return secondMean;
	}

	public int getShiftPos() {
		return shiftPos;
	}
}
