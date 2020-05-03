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


import org.chombo.util.Pair;

/**
 * Rank sum 2 sample stat
 * @author pranab
 *
 */
public class RankSumStatWindow extends SizeBoundWindow<Double> {
	private int[] rankSum = new int[2];
	protected double stat;
	protected RankedValue[] positionedValues = null;
	
	/**
	 * @author pranab
	 *
	 */
	public static class RankedValue extends Pair<Integer, Double> implements Comparable<RankedValue>{

		public RankedValue(Integer index, Double value) {
			super(index, value);
		}
		
		@Override
		public int compareTo(RankedValue that) {
			int ret =  this.right < that.right? -1 : (this.right > that.right? 1 : 0);
			return ret;
		}
	}
	
	public RankSumStatWindow() {
		super();
	}

	public RankSumStatWindow(int maxSize) {
		super(maxSize);
	}

	/* (non-Javadoc)
	 * @see org.hoidla.window.DataWindow#processFullWindow()
	 */
	public  void processFullWindow() {
		RankedValue[] rankedValues = new RankedValue[maxSize];
		for (int i = 0; i < maxSize; ++i) {
			RankedValue rankedValue = new RankedValue(i, dataWindow.get(i));
			rankedValues[i] = rankedValue;
		}
		
		//sort
		Arrays.sort(rankedValues);
		
		//swap rank and index
		positionedValues = new RankedValue[maxSize];
		for (int rank = 0; rank < maxSize; ++rank) {
			int pos = rankedValues[rank].getLeft();
			double value = rankedValues[rank].getRight();
			RankedValue positionedValue = new RankedValue(rank+1, value);
			positionedValues[pos] = positionedValue;
		}
		
		//rank sum
		rankSum[0] = rankSum[1] = 0;
		for (int i = 0; i < maxSize; ++i) {
			int rank = positionedValues[i].getLeft();
			if (i < maxSize/2) {
				rankSum[0] += rank;
			} else {
				rankSum[1] += rank;
			}
		}	
		
		//smaller
		int smSRankSum =  rankSum[0] < rankSum[1] ? rankSum[0] : rankSum[1];
		double dMaxSize = (double)maxSize ;
		double mean = dMaxSize * (dMaxSize + 1) / 4.0;
		double sd = Math.sqrt(dMaxSize * dMaxSize * (dMaxSize + 1) / 48.0);
		stat = Math.abs(smSRankSum - mean) / sd;
		
	}
	
	/**
	 * @return
	 */
	public double getStat() {
		return stat;
	}
}
