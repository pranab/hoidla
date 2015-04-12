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


package org.hoidla.window;

/**
 * Bootstrap sampler with a continuously sliding window. After a data item is added
 * window is sampled
 * @author pranab
 *
 * @param <T>
 */
public class SlidingWindowBootstrapSampler<T> extends SizeBoundWindow<T> {

	public SlidingWindowBootstrapSampler(int maxSize) {
		super(maxSize);
	}
	
	/**
	 * Sampling without replacement
	 * @return
	 */
	public T smaple() {
		int sel = (int)(dataWindow.size() * Math.random());
		return dataWindow.get(sel);
	}
}
