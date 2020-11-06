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

import java.util.Arrays;

/**
 * Finds number of neighbors within threshold distance or average of k nearest 
 * neighbors for the data point at window center
 * @author pranab
 *
 */
public class LocalNeighborhoodWindow extends SizeBoundWindow<Double> {
	private double distThreshold = -1.0;
	private double numNeighbors = -1;
	private int numNeighbosWithin;
	private double avNeighborDist;
	
	/**
	 * @param maxSize
	 */
	public LocalNeighborhoodWindow(int maxSize, double distThreshold) {
		super(maxSize);
		this.distThreshold = distThreshold;
	}

	public LocalNeighborhoodWindow(int maxSize, int numNeighbors) {
		super(maxSize);
		this.numNeighbors = numNeighbors;
	}
	
	/* (non-Javadoc)
	 * @see org.hoidla.window.DataWindow#processFullWindow()
	 */
	public  void processFullWindow() {
		int center = maxSize / 2;
		double[] distances = new double[maxSize - 1];
		int j = 0;
		for (int i = 0; i < maxSize; ++i) {
			if (i != center) {
				distances[j++]  = Math.abs(dataWindow.get(center) - dataWindow.get(i));
			}
		}
		Arrays.sort(distances);
		
		if (distThreshold > 0) {
			//number of neighbors within threshold distance
			int count = 0;
			for(int i = 0; i < distances.length; ++i) {
				if (distances[i] < distThreshold) {
					++count;
				}
			}
			numNeighbosWithin = count;
		} else {
			//average of k nearest neighbor
			double sum = 0;
			for(int i = 0; i < numNeighbors; ++i) {
				sum += distances[i];
			}
			avNeighborDist = sum / numNeighbors;
		}
	}

	public int getNumNeighbosWithin() {
		return numNeighbosWithin;
	}

	public double getAvNeighborDist() {
		return avNeighborDist;
	}
	
}
