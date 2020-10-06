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

import java.util.Arrays;

import org.chombo.stats.HistogramStat;
import org.chombo.util.Pair;
import org.chombo.util.BasicUtils;

/**
 * Two sample statistic
 * @author pranab
 *
 */
/**
 * @author pranab
 *
 */
public class TwoSampleStatistic {
	private static int NUM_BINS = 50;
	
	/**
	 * @author pranab
	 *
	 */
	private static class RankedValue extends Pair<Integer, Double> implements Comparable<RankedValue>{

		public RankedValue(Integer index, Double value) {
			super(index, value);
		}
		
		@Override
		public int compareTo(RankedValue that) {
			int ret =  this.right < that.right? -1 : (this.right > that.right? 1 : 0);
			return ret;
		}
	}

	/**
	 * @param data
	 * @param spPoint
	 * @return
	 */
	public static RankedValue[] getRankedValues(Double[] data) {
		int size = data.length;
		RankedValue[] rankedValues = new RankedValue[size];
		for (int i = 0; i < size; ++i) {
			RankedValue rankedValue = new RankedValue(i, data[i]);
			rankedValues[i] = rankedValue;
		}
		
		//sort
		Arrays.sort(rankedValues);
		
		//swap rank and index
		RankedValue[] positionedValues = new RankedValue[size];
		for (int rank = 0; rank < size; ++rank) {
			int pos = rankedValues[rank].getLeft();
			double value = rankedValues[rank].getRight();
			RankedValue positionedValue = new RankedValue(rank+1, value);
			positionedValues[pos] = positionedValue;
		}
		return positionedValues;
	}
	
	
	/**
	 * @param data
	 * @param spPoint
	 * @return
	 */
	public static double getKolmogorovSminovStat(Double[] data, int spPoint) {
		int size = data.length;
		double min = Double.MAX_VALUE;
		double max = Double.MIN_VALUE;
		for (double v : data) {
			if (v < min) {
				min = v;
			} 
			if (v > max) {
				max = v;
			} 
		}
		double binWidth = (max - min) / NUM_BINS;
		
		HistogramStat hist1 = new HistogramStat(binWidth);
		for (int i = 0; i < spPoint; ++i) {
			hist1.add(data[i]);
		}
		
		HistogramStat hist2 = new HistogramStat(binWidth);
		for (int i = spPoint; i < size; ++i) {
			hist2.add(data[i]);
		}
		
		double stat =  hist1.getKolmogorovSmirnovStatistic(hist2);
		return stat;
	}
	
	/**
	 * @param data
	 * @param spPoint
	 * @return
	 */
	public static double getCramerVonMisesStat(Double[] data, int spPoint){
		RankedValue[] positionedValues = getRankedValues(data);
		
		int size = data.length;
		int sampSize1 = spPoint;
		double sum1 = 0;
		
		for(int i = 0 ; i < sampSize1; ++i) {
			double diff = positionedValues[i].getLeft() - (i + 1);
			sum1 += (diff * diff);
		}
		sum1 *= sampSize1;
		
		int sampSize2 = size - spPoint;
		double sum2 = 0;
		for(int i = 0 ; i < sampSize2; ++i) {
			double diff = positionedValues[sampSize1 + i].getLeft() - (i + 1);
			sum2 += (diff * diff);
		}
		sum2 *= sampSize2;
		
		double u = sum1 + sum2;
		//System.out.println("u " + BasicUtils.formatDouble(u,3));
		double pr = sampSize1 * sampSize2;
		double stat = u / (pr * size) - (4.0 * pr - 1.0) / (6.0 * size);
		return stat;
	}
	
	/**
	 * @param data
	 * @param spPoint
	 * @return
	 */
	public static double getAndersonDarlingStat(Double[] data, int spPoint) {
		int size = data.length;
		Double[] clonedData = Arrays.copyOf(data, size);
		Arrays.sort(clonedData);
		int[] ranks = new int[size];
		
		//number of data point in the first sample below the ith ranked data in total sample
		for (int i = 0; i < size; ++i) {
			int count = 0;
			for(int j = 0; j < spPoint; ++j) {
				if (data[j] <= clonedData[i]) {
					++count;
				}
			}
			ranks[i] = count;
		}
		
		int m = spPoint;
		int n = size - spPoint;
		double sum = 0;
		for (int i = 1; i < size; ++i) {
			double t = ranks[i-1] * size - m * i;
			t *= t;
			t /= (i * (size - i));
			sum += t;
		}	
		double stat = sum / (m * n);
		return stat;
	}

}
