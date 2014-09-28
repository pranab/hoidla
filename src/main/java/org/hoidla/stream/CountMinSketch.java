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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.hoidla.util.EpochObjectCounter;
import org.hoidla.util.Expirer;
import org.hoidla.util.Hashing;
import org.hoidla.util.ObjectCounter;
import org.hoidla.util.SequencedObjectCounter;
import org.hoidla.util.SimpleObjectCounter;

import org.hoidla.util.Expirer;
import org.hoidla.util.ObjectCounter;

/**
 * Frequent distribution by count min sketch algorithms
 * @author pranab
 *
 */
public class CountMinSketch extends  BaseCountSketch implements FrequentItems.FrequencyDistribution {
	private static final Logger LOG = LoggerFactory.getLogger(CountMinSketch.class);
	
	/** 
	 * Constructor based on error bounds
	 * @param errorLimit
	 * @param errorProbLimit
	 */
	public CountMinSketch(double errorLimit, double errorProbLimit) {
		super(errorLimit, errorProbLimit);
		LOG.info("errorLimit:" + errorLimit + " errorProbLimit:" + errorProbLimit );
	}	
	
	public CountMinSketch(double errorLimit, double errorProbLimit, Expirer expirer) {
		super(errorLimit, errorProbLimit, expirer);
	}	

	/**
	 * Constructor  base of number of hash functions and hash value range
	 * @param width
	 * @param depth
	 */
	public CountMinSketch(int width, int depth) {
		super(width, depth);
	}


	/**
	 * Adds a value
	 * @param value
	 */
	public void add(Object value) {
		LOG.debug("item:" + value.toString());
		for (int d = 0; d < depth; ++d) {
			int w = hashFamily.hash(value,  d);
			ObjectCounter counter = sketch[d][w];
			counter.increment();
			LOG.debug("item:" + value.toString() + " current count:" + counter.getCount() );
		}
	}

	/**
	 * Adds a value
	 * @param value
	 */
	public void add(Object value, long sequence) {
		//expire
		ObjectCounter counter = null;
		for (int d = 0; d < depth; ++d) {
			for (int w = 0; w < width; ++w) {
				counter =  sketch[d][w];
				counter.expire(expirer, sequence);
			}
		}

		//increment
		for (int d = 0; d < depth; ++d) {
			int w = hashFamily.hash(value,  d);
			counter = sketch[d][w];
			counter.increment();
		}
	}

	/**
	 * Get frequency count for a value
	 * @param value
	 * @return
	 */
	public int getDistr(Object value) {
		int count = Integer.MAX_VALUE;
		for (int d = 0; d < depth; ++d) {
			int w =  hashFamily.hash(value,  d);
			int thisCount = sketch[d][w].getCount();
			if (thisCount < count) {
				count = thisCount;
			}
		}			
		LOG.debug("item:" + value.toString() + " final count:" + count );
		return count;
	}
	
	
}
