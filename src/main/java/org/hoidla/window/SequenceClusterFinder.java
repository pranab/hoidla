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
import java.util.List;
import java.util.ListIterator;


/**
 * Agglomerative hierarchical clustering
 * @author pranab
 *
 */
public class SequenceClusterFinder {
	private List<Long> sequence;
	private long maxAvInterval; 
	private long minClusterMemeber;
	private String strategy;
	private long maxInterval; 
	private List<List<Long>> clusters;
	
	public SequenceClusterFinder() {
	}
	
	/**
	 * @param sequence
	 * @param maxInterval
	 * @param minSize
	 */
	public SequenceClusterFinder(List<Long> sequence, long maxAvInterval, long maxInterval, long minSize, String strategy) {
		super();
		initialize(sequence, maxAvInterval, maxInterval, minSize, strategy);
	}
	
	/**
	 * @param sequence
	 * @param maxAvInterval
	 * @param maxInterval
	 * @param strategy
	 */
	public SequenceClusterFinder(List<Long> sequence, long maxAvInterval, long maxInterval,  String strategy) {
		super();
		initialize(sequence, maxAvInterval, maxInterval, -1, strategy);
	}

	public void initialize(List<Long> sequence, long maxAvInterval, long maxInterval, long minSize, String strategy) {
		this.sequence = sequence;
		this.maxAvInterval = maxAvInterval;
		this.maxInterval = maxInterval;
		if (minSize > 0) {
			minClusterMemeber = minSize / maxAvInterval;
		} else {
			minClusterMemeber = -1;
		}
		this.strategy = strategy;
	}

	/**
	 * @return
	 */
	public List<List<Long>> findClusters() {
		List<List<Long>> clusters = new ArrayList<List<Long>>();
		List<Long> latestcluster = null;
		
		//find clusters
		if (strategy.equals("averageInterval")) {
			for (long val : sequence) {
				if (clusters.isEmpty()) {
					latestcluster = createCluster(val, clusters);
				} else {
					boolean toAdd = false;
					if (findAverageInterVal(latestcluster, val) < maxAvInterval) {
						toAdd = true;
						if (strategy.equals("both")) {
							toAdd = (val - latestcluster.get(latestcluster.size() - 1)) < maxInterval;
						}
						//add to nearest cluster
						if (toAdd) {
							latestcluster.add(val);
						}
					} 
					if (!toAdd) {
						//create new cluster
						latestcluster = createCluster(val, clusters);
					}
				}
			}
		} else if (strategy.equals("maxInterval")) {
			for (long val : sequence) {
				if (clusters.isEmpty()) {
					latestcluster = createCluster(val, clusters);
				} else {
					if ((val - latestcluster.get(latestcluster.size() - 1)) < maxInterval) {
						latestcluster.add(val);
					} else {
						//create new cluster
						latestcluster = createCluster(val, clusters);
					}
				}	
			}
		} else {
			throw new IllegalStateException("invalid sequence clustering strategy");
		}
		
		//filter out small clusters
		if (minClusterMemeber > 0) {
			ListIterator<List<Long>> iter = clusters.listIterator();
			while (iter.hasNext()) {
				if (iter.next().size() < minClusterMemeber) {
					iter.remove();
				}
			}
		}
		this.clusters = clusters;
		return clusters;
	}
	
	/**
	 * @param val
	 * @return
	 */
	private List<Long> createCluster(long val, List<List<Long>> clusters) {
		List<Long> cluster = new ArrayList<Long>();
		cluster.add(val);
		clusters.add(cluster);
		return cluster;
	}
	
	/**
	 * @param cluster
	 * @param newData
	 * @return
	 */
	private long findAverageInterVal(List<Long> cluster, Long newData) {
		long avearge = 0;
		long sum = 0;
		if (cluster.size() > 1) {
			for (int i = 1; i < cluster.size(); ++i) {
				sum += (cluster.get(i) - cluster.get(i-1));
			}
		}
		sum += newData - cluster.get(cluster.size() - 1);
		avearge = sum / cluster.size();
		return avearge;
	}
	
	/**
	 * @return
	 */
	public List<List<Long>> getPrototypes(int minClusterMemeber) {
		List<List<Long>> prototypes = new ArrayList<List<Long>>();
		for (List<Long> cluster : clusters) {
			if (cluster.size() <= minClusterMemeber) {
				//as is
				prototypes.add(new ArrayList<Long>(cluster));
			} else {
				//find center
				long sum = 0;
				for (Long pos : cluster) {
					sum += pos;
				}
				long center = sum / cluster.size();
				
				//find left nearest
				int nearest = 0;
				for (int i = 0; i < cluster.size(); ++i) {
					if (center >=  cluster.get(i)) {
						nearest = i;
						break;
					}
				}
				
				//closest to center is prototype
				List<Long> proto = new ArrayList<Long>();
				if ((center - cluster.get(nearest)) < (cluster.get(nearest + 1) - center)) {
					proto.add(cluster.get(nearest));
				} else {
					proto.add(cluster.get(nearest+1));
				}
				prototypes.add(proto);
			}
		}
		return prototypes;
	}
}
