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

import java.util.ArrayList;
import java.util.List;

/**
 * @author pranab
 *
 */
public class SequencedObjectCounter implements ObjectCounter {
	private List<Long> sequences = new ArrayList<Long>();
	
	@Override
	public void increment() {
		throw new UnsupportedOperationException("invalid call for sequenced object counter");	
	}

	@Override
	public void increment(long sequence) {
		sequences.add(sequence);
	}

	@Override
	public void decrement() {
		sequences.remove(0);
	}

	@Override
	public void change(int amount) {
		List<Long> toBeRemoved = new ArrayList<Long>();
		for (int i  = 0; i <  amount; ++i) {
			toBeRemoved.add(sequences.get(i));
		}
		sequences.removeAll(toBeRemoved);
	}

	public int getCount() {
		return sequences.size();
	}
	public boolean isZero() {
		return sequences.size() == 0;
	}

	public void expire(Expirer expirer, long sequenceMax) {
		expirer.expire(sequences, sequenceMax);
	}
	
	public void initialize() {
		sequences.clear();
	}
}
