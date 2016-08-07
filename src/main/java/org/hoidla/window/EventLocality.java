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

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Applies different temporal locality strategies
 * @author pranab
 *
 */
public class EventLocality {
	
	/**
	 * Finds event locality score based in various strategies for positional event
	 * @param eventWindowPositions
	 * @param minOccurence
	 * @param maxIntervalAverage
	 * @param maxIntervalMax
	 * @param strategies
	 * @param windowSize
	 * @return
	 */
	public static double getPositionalEventSingleScore(List<Long> eventWindowPositions, int minOccurence, 
			long maxIntervalAverage,long maxIntervalMax, long minRangeLength, List<String> strategies, int windowSize) {
		double score = 0;
		boolean scoreSet = false;
		
		//try all strategies and quit after the first one that meets condition
		for (String strategy : strategies) {
			if (strategy.equals("numOcuurence")) {
				if (eventWindowPositions.size() > minOccurence) {
					score = 1.0;
					scoreSet = true;
				} 
			} else if (strategy.equals("averageInterval")) {
				double avInterval = 0;
				for (int j = 0; j < eventWindowPositions.size() - 1; ++j) 	{
					avInterval += (double)(eventWindowPositions.get(j+1) - eventWindowPositions.get(j));
				}
				avInterval /= (eventWindowPositions.size() - 1);
				if (avInterval <  maxIntervalAverage) {
					score = 1.0;
					scoreSet = true;
				}
			} else if (strategy.equals("maxInterval")) {
				long maxInterval = 0;
				for (int j = 0; j < eventWindowPositions.size() - 1; ++j) 	{
					long interval = eventWindowPositions.get(j+1) - eventWindowPositions.get(j);
					if (interval > maxInterval) {
						maxInterval = interval;
					}
				}
				if (maxInterval <  maxIntervalMax) {
					score = 1.0;
					scoreSet = true;
				}
			} else if (strategy.equals("rangeOccurence")) {
				long range  = findContguousOccurence(eventWindowPositions);
				if (range > minRangeLength) {
					score = 1.0;
					scoreSet = true;
				}
			}
			else {
				throw new IllegalArgumentException("invalid temporal locality strategy");
			}
			
			if (scoreSet) {
				break;
			}
		}
		
		return score;
	}
	
	/**
	 * @param eventWindowPositions
	 * @param strategies
	 * @param windowSize
	 * @return
	 */
	public static double getPositionalWeightedScore(List<Long> eventWindowPositions, int minOccurence, 
			long maxIntervalAverage,long maxIntervalMax, long minRangeLength, Map<String,Double> strategies, 
			int windowSize) {
		double score = 0;
		double weightedScore = 0;
		double weightSum = 0;
		boolean found = false;
		
		if (strategies.containsKey("numOcuurence")) {
			score = 0;
			if (eventWindowPositions.size() > minOccurence) {
				score = 1.0;
			} 

			weightedScore = score * strategies.get("numOcuurence");
			weightSum = strategies.get("numOcuurence");
			found = true;
		}
		
		if (strategies.containsKey("averageInterval")) {
			double avInterval = 0;
			for (int j = 0; j < eventWindowPositions.size() - 1; ++j) 	{
				avInterval += (double)(eventWindowPositions.get(j+1) - eventWindowPositions.get(j));
			}
			avInterval /= (eventWindowPositions.size() - 1);
			score = 0;
			if (avInterval <  maxIntervalAverage) {
				score = 1.0;				
			}

			weightedScore += score * strategies.get("averageInterval");
			weightSum += strategies.get("averageInterval");
			found = true;
		}		
		
		if (strategies.containsKey("maxInterval")) {
			long maxInterval = 0;
			for (int j = 0; j < eventWindowPositions.size() - 1; ++j) 	{
				long interval = eventWindowPositions.get(j+1) - eventWindowPositions.get(j);
				if (interval > maxInterval) {
					maxInterval = interval;
				}
			}
			score = 0;
			if (maxInterval <  maxIntervalMax) {
				score = 1.0;
			}

			weightedScore += score * strategies.get("maxInterval");
			weightSum = strategies.get("maxInterval");
			found = true;
		}		
		
		if (strategies.containsKey("rangeOccurence")) {
			long range  = findContguousOccurence(eventWindowPositions);
			score = 0;
			if (range > minRangeLength) {
				score = 1.0;
			}
			weightedScore += score * strategies.get("rangeOccurence");
			weightSum = strategies.get("rangeOccurence");
			found = true;
		}		

		if (!found) {
			throw new IllegalArgumentException("no valid temporal locality strategy found");
		}
		return weightedScore / weightSum ;
	}

	/**
	 * @param eventWindowPositions
	 * @return
	 */
	private static int findContguousOccurence(List<Long> eventWindowPositions) {
		int range = 1;
		int maxRange = 1;
		long prevPos  = eventWindowPositions.get(0);
		for (int i = 1; i < eventWindowPositions.size(); ++i) {
			if (eventWindowPositions.get(i) == prevPos) {
				++range;
			} else {
				if (range > maxRange){
					maxRange = range;
				}
				range = 1;
			}
			prevPos = eventWindowPositions.get(i);
		}
		return maxRange;
	}
	
	/**
	 * Finds event locality score based in various strategies for positional event
	 * @param eventWindowTimes
	 * @param minOccurenceTimeSpan
	 * @param maxTimeIntervalAverage
	 * @param maxTimeIntervalMax
	 * @param strategies
	 * @param windowTimeSpan
	 * @return
	 */
	public static double getTimedEventSingleScore(List<Long> eventWindowTimes, int minOccurenceTimeSpan, 
			long maxTimeIntervalAverage,long maxTimeIntervalMax, List<String> strategies, long windowTimeSpan) {
		double score = 0;
		boolean scoreSet = false;
		
		//try all strategies and quit after the first one that meets condition
		for (String strategy : strategies) {
			if (strategy.equals("numOcuurence")) {
				long span = eventWindowTimes.get(eventWindowTimes.size() - 1) - eventWindowTimes.get(0); 
				if (span > minOccurenceTimeSpan) {
					score = 1.0;
					scoreSet = true;
				} 
			} else if (strategy.equals("averageInterval")) {
				double avInterval = 0;
				for (int j = 0; j < eventWindowTimes.size() - 1; ++j) 	{
					avInterval += (double)(eventWindowTimes.get(j+1) - eventWindowTimes.get(j));
				}
				avInterval /= (eventWindowTimes.size() - 1);
				if (avInterval <  maxTimeIntervalAverage) {
					score = 1.0;
					scoreSet = true;
				}
			} else if (strategy.equals("maxInterval")) {
				long maxInterval = 0;
				for (int j = 0; j < eventWindowTimes.size() - 1; ++j) 	{
					long interval = eventWindowTimes.get(j+1) - eventWindowTimes.get(j);
					if (interval > maxInterval) {
						maxInterval = interval;
					}
				}
				if (maxInterval <  maxTimeIntervalMax) {
					score = 1.0;
					scoreSet = true;
				}
			} else {
				throw new IllegalArgumentException("invalid temporal locality strategy");
			}
			if (scoreSet) {
				break;
			}
		}
		
		return score;
	}
	
	/**
	 * @param eventWindowTimes
	 * @param minOccurenceTimeSpan
	 * @param maxTimeIntervalAverage
	 * @param maxTimeIntervalMax
	 * @param strategies
	 * @param windowTimeSpan
	 * @return
	 */
	public static double getTimedEventWeightedScore(List<Long> eventWindowTimes, int minOccurenceTimeSpan, 
			long maxTimeIntervalAverage,long maxTimeIntervalMax, Map<String,Double> strategies, long windowTimeSpan) {
		double score = 0;
		double weightedScore = 0;
		double weightSum = 0;
		boolean found = false;
		boolean scoreSet = false;
		
		if (strategies.containsKey("numOcuurence")) {
			found = true;
			long span = eventWindowTimes.get(eventWindowTimes.size() - 1) - eventWindowTimes.get(0); 
			if (span > minOccurenceTimeSpan) {
				score = 1.0;
				weightedScore = score * strategies.get("numOcuurence");
				weightSum = strategies.get("numOcuurence");
				scoreSet = true;
			} 
		}
		if (strategies.containsKey("averageInterval")) {
			found = true;
			double avInterval = 0;
			for (int j = 0; j < eventWindowTimes.size() - 1; ++j) 	{
				avInterval += (double)(eventWindowTimes.get(j+1) - eventWindowTimes.get(j));
			}
			avInterval /= (eventWindowTimes.size() - 1);
			if (avInterval <  maxTimeIntervalAverage) {
				score = 1.0;
				weightedScore += score * strategies.get("averageInterval");
				weightSum += strategies.get("averageInterval");
				scoreSet = true;
			}
		}	
		if (strategies.containsKey("maxInterval")) {
			found = true;
			long maxInterval = 0;
			for (int j = 0; j < eventWindowTimes.size() - 1; ++j) 	{
				long interval = eventWindowTimes.get(j+1) - eventWindowTimes.get(j);
				if (interval > maxInterval) {
					maxInterval = interval;
				}
			}
			if (maxInterval <  maxTimeIntervalMax) {
				score = 1.0;
				weightedScore += score * strategies.get("maxInterval");
				weightSum += strategies.get("maxInterval");
				scoreSet = true;
			}
			
		}		

		if (!found) {
			throw new IllegalArgumentException("no valid temporal locality strategy found");
		}
		
		double finalScore = 0;
		if (scoreSet) {
			finalScore = weightedScore / weightSum;
		}
		return finalScore;
	}	
	
	/**
	 * @author pranab
	 *
	 */
	public static class Context {
		public int minOccurence = 1;
		public long maxIntervalAverage = -1;
		public long maxIntervalMax = -1;
		public long minRangeLength = -1;
		public List<String> singleStatregies;
		public Map<String,Double> aggregateWeightedStrategies;
		
		
		/**
		 * @param minOccurence
		 * @param maxIntervalAverage
		 * @param maxIntervalMax
		 * @param singleStatregies
		 */
		public Context(int minOccurence, long maxIntervalAverage, long maxIntervalMax, long minRangeLength, 
				List<String> singleStatregies) {
			super();
			this.minOccurence = minOccurence;
			this.maxIntervalAverage = maxIntervalAverage;
			this.maxIntervalMax = maxIntervalMax;
			this.minRangeLength = minRangeLength;
			this.singleStatregies = singleStatregies;
		}

		
		/**
		 * @param aggregateWeightedStrategies
		 */
		public Context(Map<String, Double> aggregateWeightedStrategies) {
			super();
			this.aggregateWeightedStrategies = aggregateWeightedStrategies;
		}


		/**
		 * @param strategyList
		 */
		public void addSingleStrategy(String... strategyList) {
			singleStatregies = Arrays.asList(strategyList);
		}
		
		/**
		 * @param strategy
		 * @param weight
		 */
		public void addAggregateStrategy(String strategy, double weight) {
			if (null == aggregateWeightedStrategies) {
				aggregateWeightedStrategies = new HashMap<String,Double>();
			}
			aggregateWeightedStrategies.put(strategy, weight);
		}
	}
}
