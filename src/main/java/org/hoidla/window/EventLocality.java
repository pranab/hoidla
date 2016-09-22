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

import java.io.Serializable;
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
	private static SequenceClusterFinder clusterFinder = new SequenceClusterFinder();
	private static double EVENT_PRESENCE_SCORE = 0.5;
	private static double CLUSTER_PRESENCE_SCORE = 1.0;
	
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
			long maxIntervalAverage, boolean findClusterWithin, long minClusterSize, long maxIntervalMax, 
			long minRangeLength, List<String> strategies,int windowSize, boolean anyCond) {
		double score = EVENT_PRESENCE_SCORE;
		boolean scoreSet = false;
		Map<String, Double> scores = new HashMap<String, Double>();
		
		//try all strategies and quit after the first one that meets condition
		for (String strategy : strategies) {
			if (strategy.equals("numOccurence")) {
				if (eventWindowPositions.size() > minOccurence) {
					score = CLUSTER_PRESENCE_SCORE;
					scoreSet = true;
					if (!anyCond) {
						scores.put("numOcuurence", score);
					}
				} 
			} else if (strategy.equals("averageInterval")) {
				double avInterval = 0;
				for (int j = 0; j < eventWindowPositions.size() - 1; ++j) 	{
					avInterval += (double)(eventWindowPositions.get(j+1) - eventWindowPositions.get(j));
				}
				avInterval /= (eventWindowPositions.size() - 1);
				if (avInterval <  maxIntervalAverage) {
					score = CLUSTER_PRESENCE_SCORE;
					scoreSet = true;
					if (!anyCond) {
						scores.put("averageInterval", score);
					}
				} else {
					//find clusters within window
					if (findClusterWithin) {
						clusterFinder.initialize(eventWindowPositions, maxIntervalAverage, minClusterSize);
						if (!clusterFinder.findClusters().isEmpty()) {
							score = CLUSTER_PRESENCE_SCORE;
							scoreSet = true;
							if (!anyCond) {
								scores.put("averageInterval", score);
							}
						}
					}
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
					score = CLUSTER_PRESENCE_SCORE;
					scoreSet = true;
					if (!anyCond) {
						scores.put("maxInterval", score);
					}
				}
			} else if (strategy.equals("rangeOccurence")) {
				long range  = findContguousOccurence(eventWindowPositions);
				if (range > minRangeLength) {
					score = CLUSTER_PRESENCE_SCORE;
					scoreSet = true;
					if (!anyCond) {
						scores.put("rangeOccurence", score);
					}
				}
			}
			else {
				throw new IllegalArgumentException("invalid temporal locality strategy");
			}
			
			if (anyCond && scoreSet) {
				break;
			}
		}
		
		//and conditions
		if (!anyCond) {
			for (String strategy : strategies) {
				if (null == scores.get(strategy)) {
					score = EVENT_PRESENCE_SCORE;
					break;
				}
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
		
		if (strategies.containsKey("numOccurence")) {
			if (eventWindowPositions.size() > minOccurence) {
				score = CLUSTER_PRESENCE_SCORE;
			} else {
				score = (double)eventWindowPositions.size() / minOccurence;
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
			if (avInterval <  maxIntervalAverage) {
				score = CLUSTER_PRESENCE_SCORE;				
			} else {
				score = (double)maxIntervalAverage / avInterval;
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
			if (maxInterval <  maxIntervalMax) {
				score = CLUSTER_PRESENCE_SCORE;
			} else {
				score = (double)maxIntervalMax / maxInterval;
			}

			weightedScore += score * strategies.get("maxInterval");
			weightSum = strategies.get("maxInterval");
			found = true;
		}		
		
		if (strategies.containsKey("rangeOccurence")) {
			long range  = findContguousOccurence(eventWindowPositions);
			if (range > minRangeLength) {
				score = CLUSTER_PRESENCE_SCORE;
			} else {
				score = (double)range / minRangeLength;
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
			long maxTimeIntervalAverage,boolean findClusterWithin, long minClusterSize, long maxTimeIntervalMax, 
			List<String> strategies, long windowTimeSpan,boolean anyCond) {
		double score = EVENT_PRESENCE_SCORE;
		boolean scoreSet = false;
		Map<String, Double> scores = new HashMap<String, Double>();
		
		//try all strategies and quit after the first one that meets condition
		for (String strategy : strategies) {
			if (strategy.equals("numOccurence")) {
				long occurences = eventWindowTimes.size();
				//System.out.println("occurences: " + occurences);
				if (occurences > minOccurenceTimeSpan) {
					score = CLUSTER_PRESENCE_SCORE;
					scoreSet = true;
					if (!anyCond) {
						scores.put("numOccurence", score);
					}
				} 
			} else if (strategy.equals("averageInterval")) {
				double avInterval = 0;
				for (int j = 0; j < eventWindowTimes.size() - 1; ++j) 	{
					avInterval += (double)(eventWindowTimes.get(j+1) - eventWindowTimes.get(j));
				}
				avInterval /= (eventWindowTimes.size() - 1);
				if (avInterval <  maxTimeIntervalAverage) {
					score = CLUSTER_PRESENCE_SCORE;
					scoreSet = true;
					if (!anyCond) {
						scores.put("averageInterval", score);
					}
				} else {
					//find clusters within window
					if (findClusterWithin) {
						clusterFinder.initialize(eventWindowTimes, maxTimeIntervalAverage, minClusterSize);
						if (!clusterFinder.findClusters().isEmpty()) {
							score = CLUSTER_PRESENCE_SCORE;
							scoreSet = true;
							if (!anyCond) {
								scores.put("averageInterval", score);
							}
						}
					}
				}
			} else if (strategy.equals("maxInterval")) {
				long maxInterval = 0;
				for (int j = 0; j < eventWindowTimes.size() - 1; ++j) 	{
					long interval = eventWindowTimes.get(j+1) - eventWindowTimes.get(j);
					if (interval > maxInterval) {
						maxInterval = interval;
					}
				}
				//System.out.println("maxInterval: " + maxInterval);
				if (maxInterval <  maxTimeIntervalMax) {
					score = CLUSTER_PRESENCE_SCORE;
					scoreSet = true;
					if (!anyCond) {
						scores.put("maxInterval", score);
					}
				}
			} else {
				throw new IllegalArgumentException("invalid temporal locality strategy");
			}
			
			if (anyCond && scoreSet) {
				break;
			}
		}

		//and conditions
		if (!anyCond) {
			for (String strategy : strategies) {
				if (null == scores.get(strategy)) {
					//System.out.println("failed in AND condition");
					score = EVENT_PRESENCE_SCORE;
					break;
				}
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
		double score = EVENT_PRESENCE_SCORE;
		double weightedScore = 0;
		double weightSum = 0;
		boolean found = false;
		boolean scoreSet = false;
		
		if (strategies.containsKey("numOcuurence")) {
			found = true;
			long occurences = eventWindowTimes.size();
			if (occurences > minOccurenceTimeSpan) {
				score = CLUSTER_PRESENCE_SCORE;
			} else {
				score = (double)occurences / minOccurenceTimeSpan;
			}
			weightedScore = score * strategies.get("numOcuurence");
			weightSum = strategies.get("numOcuurence");
			scoreSet = true;
		}
		if (strategies.containsKey("averageInterval")) {
			found = true;
			double avInterval = 0;
			for (int j = 0; j < eventWindowTimes.size() - 1; ++j) 	{
				avInterval += (double)(eventWindowTimes.get(j+1) - eventWindowTimes.get(j));
			}
			avInterval /= (eventWindowTimes.size() - 1);
			
			if (avInterval <  maxTimeIntervalAverage) {
				score = CLUSTER_PRESENCE_SCORE;
			} else {
				score = (double)maxTimeIntervalAverage / avInterval;
			}
			weightedScore += score * strategies.get("averageInterval");
			weightSum += strategies.get("averageInterval");
			scoreSet = true;
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
				score = CLUSTER_PRESENCE_SCORE;
			} else {
				score = (double)maxTimeIntervalMax / maxInterval;
			}
			weightedScore += score * strategies.get("maxInterval");
			weightSum += strategies.get("maxInterval");
			scoreSet = true;
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
	public static class Context implements Serializable {
		public int minOccurence = 1;
		public long maxIntervalAverage = -1;
		boolean findClusterWithin;
		long minClusterSize = -1;
		public long maxIntervalMax = -1;
		public long minRangeLength = -1;
		public List<String> singleStatregies;
		public Map<String,Double> aggregateWeightedStrategies;
		public boolean anyCond;
		

		/**
		 * @param minOccurence
		 * @param maxIntervalAverage
		 * @param maxIntervalMax
		 * @param singleStatregies
		 */
		public Context(int minOccurence, long maxIntervalAverage, boolean findClusterWithin, long minClusterSize, 
				long maxIntervalMax, long minRangeLength, List<String> singleStatregies, boolean anyCond) {
			super();
			this.minOccurence = minOccurence;
			this.maxIntervalAverage = maxIntervalAverage;
			this.findClusterWithin = findClusterWithin;
			this.minClusterSize = minClusterSize;
			this.maxIntervalMax = maxIntervalMax;
			this.minRangeLength = minRangeLength;
			this.singleStatregies = singleStatregies;
			this.anyCond = anyCond;
		}
		
		/**
		 * @param minOccurence
		 * @param maxIntervalAverage
		 * @param maxIntervalMax
		 * @param singleStatregies
		 */
		public Context(int minOccurence, long maxIntervalAverage, long maxIntervalMax, long minRangeLength, 
				List<String> singleStatregies, boolean anyCond) {
			super();
			this.minOccurence = minOccurence;
			this.maxIntervalAverage = maxIntervalAverage;
			this.maxIntervalMax = maxIntervalMax;
			this.minRangeLength = minRangeLength;
			this.singleStatregies = singleStatregies;
			this.anyCond = anyCond;
		}

		/**
		 * @param minOccurence
		 * @param maxIntervalAverage
		 * @param maxIntervalMax
		 * @param singleStatregies
		 */
		public Context(int minOccurence, long maxIntervalAverage, long maxIntervalMax, long minRangeLength, 
				Map<String,Double> aggregateWeightedStrategies) {
			super();
			this.minOccurence = minOccurence;
			this.maxIntervalAverage = maxIntervalAverage;
			this.maxIntervalMax = maxIntervalMax;
			this.minRangeLength = minRangeLength;
			this.aggregateWeightedStrategies = aggregateWeightedStrategies;
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
