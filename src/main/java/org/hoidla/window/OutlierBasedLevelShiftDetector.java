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

import org.hoidla.util.TimeStampedTaggedDouble;

/**
 * @author pranab
 *
 */
public class OutlierBasedLevelShiftDetector extends SizeBoundWindow<TimeStampedTaggedDouble> {
	private String outlierLabel;
	private double mean;
	private int maxToleranace;
	private boolean levelShifted;
	private long levelShiftStart;
	private long levelShiftEnd;
	private boolean newLevelShift;
	

	public OutlierBasedLevelShiftDetector(int maxSize) {
		super(maxSize);
	}
	
	public OutlierBasedLevelShiftDetector withOutlierLabel(String outlierLabel) {
		this.outlierLabel = outlierLabel;
		return this;
	}
	
	public OutlierBasedLevelShiftDetector withMean(double mean) {
		this.mean = mean;
		return this;
	}
	
	public void processFullWindow() {
		int normCount = 0;
		int aboveCount = 0;
		int belowCount = 0;
		
		for (TimeStampedTaggedDouble obj : dataWindow) {
			if (!obj.getTag().equals(outlierLabel)) {
				++normCount;
			}
			if (obj.getTag().equals(outlierLabel) && obj.getValue() > mean) {
				++aboveCount;
			}
			if (obj.getTag().equals(outlierLabel) && obj.getValue() < mean) {
				++belowCount;
			}
		}
		int violationCount = aboveCount < belowCount ? aboveCount : belowCount;
		violationCount += normCount;
		
		newLevelShift = false;
		if (violationCount <= maxToleranace) {
			if (!levelShifted) {
				levelShiftStart = dataWindow.get(0).getTimestamp();
				levelShifted = true;
				newLevelShift = true;
			}
			levelShiftEnd = dataWindow.get(dataWindow.size()-1).getTimestamp();
		} else {
			levelShifted = false;
		}
	}

	public boolean isLevelShifted() {
		return levelShifted;
	}

	public long getLevelShiftStart() {
		return levelShiftStart;
	}

	public long getLevelShiftEnd() {
		return levelShiftEnd;
	}

	public boolean isNewLevelShift() {
		return newLevelShift;
	}
}
