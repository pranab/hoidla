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

package org.hoidla.util;

import java.util.ArrayList;
import java.util.List;

/**
 * Cluster of temporal sequence data
 * @author pranab
 *
 */
public class TemporalCluster {
	private List<Long> eventTimes = new ArrayList<Long>();
	private boolean startFound;
	private boolean endFound;
	private Long averageGap;
	
	public void add(long eventTime) {
		eventTimes.add(eventTime);
	}

	public boolean isStartFound() {
		return startFound;
	}

	public void setStartFound(boolean startFound) {
		this.startFound = startFound;
	}

	public boolean isEndFound() {
		return endFound;
	}

	public void setEndFound(boolean endFound) {
		this.endFound = endFound;
	}
	
	public long getStartTime() {
		return eventTimes.get(0);
	}
	
	public long getEndTime() {
		return eventTimes.get(eventTimes.size() - 1);
	}
	
	public int getSize() {
		return eventTimes.size();
	}
	
	public long findAverageGap() {
		if (null == averageGap) {
			long sum = 0;
			for (int i = 1; i < eventTimes.size(); ++i) {
				sum += eventTimes.get(i) - eventTimes.get(i - 1);
			}
			averageGap = sum / (eventTimes.size() - 1);
		}
		
		return averageGap;
	}
}
