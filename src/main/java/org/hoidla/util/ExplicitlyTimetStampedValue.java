/*
 * hoidla: various streaming algorithms for Big Data solutions
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
 * @author pranab
 *
 * @param <T>
 */
public class ExplicitlyTimetStampedValue<T>  extends ExplicitlyTimeStampedFlag {
	private T value;
	
	/**
	 * @param value
	 * @param timeStamp
	 */
	public ExplicitlyTimetStampedValue(T value, long timeStamp, boolean flag) {
		super(timeStamp,  flag);
		this.value = value;
	}

	/**
	 * @return
	 */
	public T getValue() {
		return value;
	}
	

}
