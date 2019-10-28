/*
 * hoidla: various algorithms for sequence data solutions
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

package org.hoidla.analyze;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * @author pranab
 *
 */
public class ChangePointDetection implements Serializable {
	private double[] data;
	
	public ChangePointDetection(double[] data) {
		this.data = data;
	}
	
	/**
	 * @param winHalfSize
	 * @param numChangePoints
	 */
	public ChangePoint[] detectByWindow(int winHalfSize, int numChangePoints) {
		int size = data.length - 2 * winHalfSize;
		ChangePoint[] changePoints = new ChangePoint[size];
		int beg = 0;
		int center = winHalfSize;
		int end = 2 * winHalfSize;
		
		for (int i = 0; end < data.length; ++beg, ++center, ++end, ++i) {
			double[] leftCdf = countSegment(data,beg, center);
			double leftCost = findCost(leftCdf, beg, center);
			double[] rightCdf = countSegment(data,center, end);
			double rightCost = findCost(rightCdf, center, end);
			double[] allCdf = countSegment(data,beg, end);
			double allCost = findCost(allCdf, beg, end);
			double disc = allCost - (leftCost + rightCost);
			changePoints[i] = new ChangePoint(center, disc);
		}
		
		//sort descending and return top
		return getTopChangePoints(changePoints, numChangePoints);
	}
	
	/**
	 * @param numChangePoints
	 * @return
	 */
	public ChangePoint[] detectByBinarySearch(int numChangePoints, int minSegmenLength) {
		List<ChangePoint> changePointsList = new ArrayList<ChangePoint>();
		detectByBinarySearchHelper(data,  numChangePoints,  minSegmenLength, changePointsList);
		
		ChangePoint[] changePoints = new ChangePoint[changePointsList.size()];
		changePoints = changePointsList.toArray(changePoints);
		
		//sort descending and return top
		return getTopChangePoints(changePoints, numChangePoints);
	}
	
	/**
	 * @param data
	 * @param numChangePoints
	 * @param minSegmenLength
	 * @param changePoints
	 */
	private void detectByBinarySearchHelper(double[] data, int numChangePoints, int minSegmenLength, 
			List<ChangePoint> changePoints) {
		if (data.length > 2 * minSegmenLength) {
			int cpIndex = 0;
			double maxDisc = 0;
			for (int sp = minSegmenLength; sp < data.length - minSegmenLength; ++sp) {
				double[] leftCdf = countSegment(data, 0, sp);
				double leftCost = findCost(leftCdf, 0, sp);
				double[] rightCdf = countSegment(data,sp, data.length);
				double rightCost = findCost(rightCdf, sp, data.length);
				double[] allCdf = countSegment(data, 0, data.length);
				double allCost = findCost(allCdf, 0, data.length);
				double disc = allCost - (leftCost + rightCost);
				if (disc > maxDisc) {
					maxDisc = disc;
					cpIndex = sp;
				}
			}
			
			//add change point
			changePoints.add(new ChangePoint(cpIndex, maxDisc));
			
			//split and recurse
			double[] leftData = Arrays.copyOfRange(data, 0, cpIndex);
			double[] rightData = Arrays.copyOfRange(data, cpIndex, data.length);
			detectByBinarySearchHelper(leftData, numChangePoints,  minSegmenLength,changePoints);
			detectByBinarySearchHelper(rightData, numChangePoints,  minSegmenLength, changePoints);
		}
	}
	
	/**
	 * @param changePoints
	 * @param numChangePoints
	 * @return
	 */
	private ChangePoint[] getTopChangePoints(ChangePoint[] changePoints, int numChangePoints) {
		int size = changePoints.length;
		Arrays.sort(changePoints);
		ChangePoint[] topChangePonts = null;
		if (size <= numChangePoints) {
			topChangePonts = changePoints;
		} else {
			topChangePonts = Arrays.copyOfRange(changePoints, 0, numChangePoints);
		}
		return topChangePonts;
	}
	
	/**
	 * @param beg
	 * @param end
	 * @return
	 */
	private double[] countSegment(double[] data, int beg, int end) {
		double[] counts = new double[data.length];
		for (int i = 0; i < data.length; ++i) {
			double count = 0;
			for (int j = beg; j < end; ++j) {
				if (data[j] < data[i]) {
					++count;
				} else if (data[j] == data[i]) {
					count += 0.5;
				}
			}
			count /= (end - beg);
			counts[i] = count;
		}
		return counts;
	}
	
	/**
	 * @param cdf
	 * @param beg
	 * @param end
	 * @return
	 */
	private double findCost(double[] cdf, int beg, int end) {
		//max likelihood cost
		double cost = 0;
		for (int i = 0; i < cdf.length; ++i) {
			double inv = 1.0 - cdf[i];
			double temp = cdf[i] * Math.log(cdf[i]) + inv * Math.log(inv);
			int j = i + 1;
			temp /= ((j  - 0.5) * (cdf.length - j + 0.5));
			cost += temp;
		}
		cost *= (beg - end);
		return cost;
	}

}
