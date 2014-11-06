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
 * Time stamp set at construction time
 * @author pranab
 *
 */
public class ImplicitlyTimeStamped implements TimeStamped {
	private Object value;
	private long timeStamp;
	
	
	public ImplicitlyTimeStamped(Object value) {
		super();
		this.value = value;
		timeStamp = System.currentTimeMillis();
	}

	@Override
	public long getTimeStamp() {
		return timeStamp;
	}

	public Object getValue() {
		return value;
	}

}
