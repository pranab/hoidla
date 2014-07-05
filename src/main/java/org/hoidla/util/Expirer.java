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

import java.util.List;
import java.util.ListIterator;

/**
 * expires items from a list
 * @author pranab
 *
 */
public class Expirer {
	private long window;
	
	public Expirer(long window) {
		this.window = window;
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
	
}
