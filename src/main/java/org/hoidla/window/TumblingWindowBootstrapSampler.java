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
 * Tumbling window boor strap sampler. When window is full, it's sampled without
 * replacement
 * @author pranab
 *
 * @param <T>
 */
public class TumblingWindowBootstrapSampler<T> extends SizeBoundWindow<T> {
	private int sampleIter;
	
	public TumblingWindowBootstrapSampler(int maxSize) {
		super(maxSize);
		setProcessStepSize(maxSize);
		setStepSize(maxSize);
	}

	/**
	 * 
	 */
	public void startSampling() {
		sampleIter = 0;
	}
	
	/**
	 * @return true if there are more samples
	 */
	public boolean hasSamples(){
		return sampleIter < maxSize;
	}
	
	/**
	 * @return next sample
	 */
	public T nextSample() {
		int sel = (int)(Math.random() * maxSize);
		++sampleIter;
		return dataWindow.get(sel);
	}
	
}
