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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author pranab
 *
 */
public class SimpleObjectCounter  implements ObjectCounter {
	protected int count;
	private static final Logger LOG = LoggerFactory.getLogger(SimpleObjectCounter.class);
	
	
	@Override
	public void increment() {
		++count;
	}

	@Override
	public void increment(long sequence) {
		throw new UnsupportedOperationException("invalid call for simple object counter");	
	}

	@Override
	public void  decrement() {
		--count;
}

	@Override
	public void change(int amount) {
		count += amount;
	}

	public int getCount() {
		return count;
	}

	public boolean isZero() {
		return count == 0;
	}

	public void expire(Expirer expirer, long sequenceMin) {
		throw new UnsupportedOperationException("invalid call for simple object counter");	
	}
	
	public void initialize() {
		count = 0;
	}
}
