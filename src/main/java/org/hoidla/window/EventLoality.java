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

import java.util.List;

/**
 * @author pranab
 *
 */
public class EventLoality {
	
	/**
	 * @param eventWindowPositions
	 * @param minOccurence
	 * @param maxIntervalAverage
	 * @param maxIntervalMax
	 * @param windowSize
	 * @return
	 */
	public static double getScore(List<Long> eventWindowPositions, int minOccurence, long maxIntervalAverage,
			long maxIntervalMax, int windowSize) {
		double score = 0;
		boolean scoreSet = false;
		//check for min occurences
		if (eventWindowPositions.size() < minOccurence) {
			score = 0;
			scoreSet = true;
		} 
		
		//check average interval if limit set
		if (!scoreSet && maxIntervalAverage > 0) {
			double avInterval = 0;
			for (int j = 0; j < eventWindowPositions.size() - 1; ++j) 	{
				avInterval += (double)(eventWindowPositions.get(j+1) - eventWindowPositions.get(j));
			}
			avInterval /= (eventWindowPositions.size() - 1);
			if (avInterval > maxIntervalAverage) {
				score = 0;
				scoreSet = true;
			}
		}
		
		//check max interval if limit set
		if (!scoreSet && maxIntervalMax > 0) {
			long maxInterval = 0;
			for (int j = 0; j < eventWindowPositions.size() - 1; ++j) 	{
				long interval = eventWindowPositions.get(j+1) - eventWindowPositions.get(j);
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
			score = (double)eventWindowPositions.size() / windowSize;
		}

		return score;
	}
}
