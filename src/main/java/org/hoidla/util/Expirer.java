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

import java.util.LinkedList;
import java.util.List;
import java.util.ListIterator;


/**
 * expires items from a list
 * @author pranab
 *
 */
public class Expirer {
	private long window;
	private int maxEpochs;
	
	protected enum ExpiryPolicy {
		Sequence,
		Epoch
	}
	private ExpiryPolicy expiryPolicy;
	
	//hash family
	protected  Hashing.MultiHashFamily hashFamily;
	
	/**
	 * 
	 */
	public Expirer(int maxEpochs) {
		this.maxEpochs = maxEpochs;
		expiryPolicy = ExpiryPolicy.Epoch;
	}
	
	/**
	 * @param window
	 */
	public Expirer(long window) {
		this.window = window;
		expiryPolicy = ExpiryPolicy.Sequence;
	}
	
	public boolean isSequenceExpirer() {
		return expiryPolicy == ExpiryPolicy.Sequence;
	}
	
	/**
	 * @param values
	 * @param current
	 */
	public void expire(List<Long> values, long current) {
		long earliest = current - window;
		if (values.get(0) < earliest) {
			ListIterator<Long> iter =  values.listIterator();
			while (iter.hasNext()) {
				if (iter.next() < earliest) {
					iter.remove();
				}
			}
		}
	}
	
	/**
	 * @param count
	 * @param epochs
	 * @param maxEpochs
	 * @return
	 */
	public int expire(int count, LinkedList<Integer> epochs) {
		if (count > 0) {
			if (epochs.size() > 0) {
				int epochCounts = 0;
				for (int epoch : epochs) {
					epochCounts += epoch;
				}
				int newEpoch = count - epochCounts;
				epochs.add(newEpoch);
				if (epochs.size() > maxEpochs) {
					int oldestEpoch = epochs.remove();
					count -= oldestEpoch;
				}
			} else {
				int newEpoch = count ;
				epochs.add(newEpoch);
			}
		}
		return count;
	}
	
}
