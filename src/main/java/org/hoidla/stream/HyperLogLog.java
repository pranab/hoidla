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


package org.hoidla.stream;

import org.hoidla.stream.UniqueItems.UniqueItemsCounter;
import org.hoidla.util.Hashing;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Hyper Log Log algorithm for cardinality estimation by Flajolet, Fusy, Gandouet and 
 * @author pranab
 *
 */
public class HyperLogLog implements UniqueItemsCounter {
	private int numBucketBits;
	private int bucketCount;
	private Hashing.MultiHashFamily hashFamily;
	private int[] buckets;
	private double biasCorrection;
	private long count;
	private static final Logger LOG = LoggerFactory.getLogger(HyperLogLog.class);
	
	/**
	 * @param numBucketBits
	 */
	public HyperLogLog(int numBucketBits) {
		//validity check
		this.numBucketBits = numBucketBits;
		final int minBits = 4;
		final int maxBits = Integer.SIZE * 3 / 4;
		this.numBucketBits = this.numBucketBits < minBits ? minBits : 
			(this.numBucketBits > maxBits ? maxBits : this.numBucketBits);
		
		//set up buckets
		bucketCount = 1 << this.numBucketBits;
		buckets = new int[bucketCount];
		intializeBuckets();
		calculateBiasCorrection();
		count = 0;
	}

	/**
	 * @param relStdDev
	 */
	public HyperLogLog(double relStdDev) {
		this((int)(Math.log((1.106 / relStdDev) * (1.106 / relStdDev)) / Math.log(2)));
	}	
	
	/**
	 * 
	 */
	private void intializeBuckets() {
		for (int i = 0; i < bucketCount; ++i) {
			buckets[i] = 0;
		}
	}
	
	/**
	 * 
	 */
	private void calculateBiasCorrection() {
		double c = Math.log(2);
		biasCorrection = 1.0 / (2.0 * c  * (1.0 + (3.0 * c - 1) / bucketCount));
		LOG.info("biasCorrection:" + biasCorrection);
	}
	
	/* (non-Javadoc)
	 * @see org.hoidla.stream.UniqueItems.UniqueItemsCounter#getUnqueCount()
	 */
	@Override
	public long getUnqueCount() {
		double sum = 0;
		int zeroCounts = 0;
		for (int i = 0; i < bucketCount; ++i) {
			sum += 1.0 / (1 << buckets[i]);
			if (buckets[i] == 0) {
				++zeroCounts;
			}
		}
		long uniqueCount =  Math.round(biasCorrection / sum);
		LOG.info("getUniqueCount sum:" + sum + " uniqueCount:" + uniqueCount + " zeroCounts:" + zeroCounts);
		if (uniqueCount < 2.5 * bucketCount) {
			uniqueCount = countiLinear(zeroCounts);
			LOG.info("using linear counting");
		}
		return uniqueCount;
	}

	/**
	 * Estimator for small cardinality range
	 * @param zeroCounts
	 * @return
	 */
	private long countiLinear(int zeroCounts) {
		return Math.round(bucketCount * Math.log((double)bucketCount / zeroCounts));
	}
	
	/* (non-Javadoc)
	 * @see org.hoidla.stream.UniqueItems.UniqueItemsCounter#add(java.lang.Object)
	 */
	@Override
	public void add(Object value) {
		int hash = value.hashCode();
		if (hash < 0){
			hash = - hash;
		}
		
		//bucket based on numBucketBits MS bits
		int bucketIndex = hash >>> (Integer.SIZE  - numBucketBits);
		
		//hash for the bucket using remaining
		int bucketHash = hash << numBucketBits | (bucketCount -1);
		int leadZeros = Integer.numberOfLeadingZeros(bucketHash);
		LOG.info("bucketIndex:" + bucketIndex + " leadZeros:" + leadZeros);
		
		//update
		if (leadZeros > buckets[bucketIndex]) {
			buckets[bucketIndex] = leadZeros;
		}
		
		++count;
	}

	public long getCount() {
		return count;
	}

	@Override
	public void clear() {
		intializeBuckets();
		count = 0;
	}

}
