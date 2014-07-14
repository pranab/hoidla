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

package org.hoidla.util;

/**
 * Biased coin flipper
 * @author pranab
 *
 */
public class BiasedCoinFlipper {
	private double bias;

	/**
	 * @param bias
	 */
	public BiasedCoinFlipper(double bias) {
		super();
		this.bias = bias;
	}

	/**
	 * @param bias
	 */
	public void setBias(double bias) {
		this.bias = bias;
	}
	
	/**
	 * @return
	 */
	public boolean flip() {
		return Math.random() <= bias; 
	}
	
	/**
	 * @param maxCount
	 * @return
	 */
	public int flipTillTail(int maxCount) {
		int count = 0;
		boolean isHead = true;
		while (isHead && count <= maxCount) {
			isHead = Math.random() <= bias;
			++count;
		} ;
		if (!isHead)
				--count;
		return count;
	}
	
}
