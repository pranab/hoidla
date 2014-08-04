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

import java.util.BitSet;

import org.hoidla.util.Hashing;

/**
 * @author pranab
 *
 */
public class BloomFilter {
	private int bitVectorSize;
	private int hashFamilySize;
	private BitSet filter;
	protected  Hashing.MultiHashFamily hashFamily;
	
	/**
	 * @param maxSetSize
	 * @param falsePositiveProb
	 */
	public BloomFilter(int maxSetSize, double falsePositiveProb) {
		double c = Math.log(2);
		bitVectorSize = (int)Math.round((-maxSetSize * Math.log(falsePositiveProb) / (c * c)));
		
		hashFamilySize = (int)Math.round(c *  bitVectorSize / maxSetSize) ;
		filter = new BitSet(bitVectorSize);
		hashFamily = new Hashing.MultiHashFamily(bitVectorSize, hashFamilySize);
	}

	/**
	 * Adds new object
	 * @param value
	 */
	public void add(Object value) {
		for (int h = 0; h < hashFamilySize; ++h) {
			int bucket = hashFamily.hash(value,  h);
			filter.set(bucket);
		}
	}
	
	/**
	 * @param value
	 * @return
	 */
	public boolean exists(Object value) {
		boolean doesExist = true;
		for (int h = 0; h < hashFamilySize; ++h) {
			int bucket = hashFamily.hash(value,  h);
			if (!filter.get(bucket)) {
				doesExist = false;
				break;
			}
		}		
		return doesExist;
	}
	
}
