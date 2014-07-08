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

import java.io.UnsupportedEncodingException;


/**
 * Approximate probabilstic frequency distribution
 * @author pranab
 *
 */
public class FrequencyDistribution {
	
	/**
	 * @author pranab
	 *
	 */
	public static  class CountMinSketch{
		//sketch
		protected int width;
		protected int depth;
		protected int[][] sketch;
		
		//hash family
		protected int[] a;
		protected int[] b;
		
		//large prime
		protected int c = 1000099;

		/** 
		 * Constructor based on error bounds
		 * @param errorLimit
		 * @param errorProbLimit
		 */
		public CountMinSketch(double errorLimit, double errorProbLimit) {
			width = (int)Math.round(2.0 / errorLimit);
			depth = (int)Math.round(Math.log(errorProbLimit));
			initialize(width, depth);
		}	
		
		/**
		 * Constructor  base of number of hash functions and hash value range
		 * @param width
		 * @param depth
		 */
		public CountMinSketch(int width, int depth) {
			initialize(width, depth);
		}

		/**
		 * @param width
		 * @param depth
		 */
		public void initialize(int width, int depth) {
			this.width = width;
			this.depth = depth;
			sketch = new int[depth][width];
			a = new int[depth];
			b = new int[depth];

			//initialize
			for (int i = 0; i < depth; ++i) {
				a[i] = (int)(Math.random() * c);
				b[i] = (int)(Math.random() * c);
				for (int j = 0; j < width; ++j) {
					sketch[i][j] = 0;
				}
			}
		}
			
		/**
		 * Adds a value
		 * @param value
		 */
		public void add(Object value) {
			for (int d = 0; d < depth; ++d) {
				int w = hash(value,  d);
				++sketch[d][w];
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
				int w = hash(value,  d);
				if (sketch[d][w] < count) {
					count = sketch[d][w];
				}
			}			
			return count;
		}
		
		/**
		 * hash for d th hash function
		 * @param value
		 * @param d  
		 * @return
		 */
		protected  int hash(Object value, int d) {
			int hashCode = 0;
			if (value instanceof String) {
				byte[] bytes = null;
				try {
					bytes = ((String)value).getBytes("utf-8");
				} catch (UnsupportedEncodingException e) {
					throw new IllegalArgumentException("failed to decode string into byte array" + e.getMessage());
				}
				int accum = 0;
				for (int i =0; i < bytes.length; ++i) {
					accum ^= bytes[i] * a[d];
				}
				accum ^= b[d];
				hashCode =  (accum % c) % width;
			} else if (value instanceof Integer) {
				Integer valInt = (Integer)value;
				int accum = 0;
				for (int i =0; i < 4; ++i) {
					accum ^= (valInt & 0x000F)  * a[d];
					valInt >>= 8;
				}
				accum ^= b[d];
				hashCode =  (accum % c) % width;
			}
			return hashCode;
		}

	}

}
