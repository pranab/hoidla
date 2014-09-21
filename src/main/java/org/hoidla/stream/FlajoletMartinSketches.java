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

import org.hoidla.util.Hashing;

/**
 * Finds number of unique items using Flajolet and Martin sketches algorithm
 * @author pranab
 *
 */
public class FlajoletMartinSketches implements UniqueItems.UniqueItemsCounter {
	private int numCounters;
	private Hashing.MultiHashFamily hashFamily;
	private long[] counters;
	
	public FlajoletMartinSketches(double errorLimit, double errorProbLimit) {
		int numCounters = (int)(1.0 / (errorLimit * errorLimit * Math.log(1.0 / errorProbLimit)));
		hashFamily = new Hashing.MultiHashFamily(numCounters);
		counters = new long[numCounters];
		clear();
	}
	
	/**
	 * Adds a value
	 * @param value
	 */
	public void add(Object value) {
		for (int i = 0; i < numCounters; ++i) {
			int hash = hashFamily.hash(value,  i);
			if (hash > 0) {
				int k = 0;
				long bitmask = 1;
				for (int j = 0; j < 32 && ((hash & 1) == 0); ++j, ++k) {
					hash >>= 1;
					bitmask <<= 1;
				}
				counters[i] |= bitmask;
			}
		}
	}

	@Override
	public long getUnqueCount() {
		int sumZeroPos = 0;
		for (int i = 0; i < numCounters; ++i) {
			long c = counters[i];
			int lsZero = 0;
			for (int j = 0; j < 64 && ((c & 1) == 1); ++j) {
				c >>= 1;
				++lsZero;
			}
			sumZeroPos += lsZero;
		}
		double avZeroPos = ((double)sumZeroPos) / numCounters;
		long count = (long)(1.298 * Math.pow(2.0, avZeroPos));
		return count;
	}	
	
	public void clear() {
		for (int i = 0; i < numCounters; ++i) {
			counters[i] = 0;
		}
	}
}
