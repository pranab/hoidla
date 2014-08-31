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

public interface ObjectCounter {
	
	/**
	 *  increment
	 */
	public void increment();
	
	/**
	 * increment sequenced object
	 * @param sequence
	 */
	public void increment(long sequence);
	
	/**
	 * decrement
	 * @return
	 */
	public void decrement();

	/**
	 * change count
	 * @param count
	 * @return
	 */
	public void change(int amount);
	
	/**
	 * @return
	 */
	public int getCount();
	
	/**
	 * @return
	 */
	public boolean isZero();
	
	/**
	 * @param expirer
	 */
	public void expire(Expirer expirer, long sequenceMax);
	
	/**
	 * 
	 */
	public void initialize();
	
}
