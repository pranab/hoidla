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
	private long maxInterval; 
	private long minClusterMemeber;
	
	public SequenceClusterFinder() {
	}
	
	/**
	 * @param sequence
	 * @param maxInterval
	 * @param minSize
	 */
	public SequenceClusterFinder(List<Long> sequence, long maxInterval, long minSize) {
		super();
		this.sequence = sequence;
		this.maxInterval = maxInterval;
		minClusterMemeber = minSize / maxInterval;
	}
	
	public void initialize(List<Long> sequence, long maxInterval, long minSize) {
		this.sequence = sequence;
		this.maxInterval = maxInterval;
		minClusterMemeber = minSize / maxInterval;
	}

	/**
	 * @return
	 */
	public List<List<Long>> findClusters() {
		List<List<Long>> clusters = new ArrayList<List<Long>>();
		List<Long> latestcluster = null;
		
		//find clusters
		for (long val : sequence) {
			if (clusters.isEmpty()) {
				latestcluster = createCluster(val, clusters);
			} else {
				if ((val - latestcluster.get(latestcluster.size() -1)) < maxInterval) {
					//add to nearest cluster
					latestcluster.add(val);
				} else {
					//create new cluster
					latestcluster = createCluster(val, clusters);
				}
			}
		}
		
		//filter out small clusters
		ListIterator<List<Long>> iter = clusters.listIterator();
		while (iter.hasNext()) {
			if (iter.next().size() < minClusterMemeber) {
				iter.remove();
			}
		}
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
	
	
}
