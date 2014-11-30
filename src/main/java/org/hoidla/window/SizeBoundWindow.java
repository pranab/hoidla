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
 * Sliding window bounded my a max size
 * @author pranab
 *
 */
public class SizeBoundWindow<T> extends DataWindow<T>{
	private int maxSize;
	private int stepSize = 1;
	private int processStepSize = 1;
	
	/**
	 * @param maxSize
	 */
	public SizeBoundWindow(int maxSize) {
		super(true);
		this.maxSize = maxSize;
	}
	
	/**
	 * @param maxSize
	 * @param stepSize
	 */
	public SizeBoundWindow(int maxSize, int stepSize) {
		this(maxSize);
		this.stepSize = stepSize;
	}
	
	/**
	 * @param maxSize
	 * @param stepSize
	 */
	public SizeBoundWindow(int maxSize, int stepSize, int processStepSize) {
		this(maxSize, stepSize);
		this.processStepSize = processStepSize;
	}

	/* (non-Javadoc)
	 * @see org.hoidla.window.DataWindow#expire()
	 */
	public void expire() {
		if (dataWindow.size() > maxSize) {
			//process window data
			if (count % processStepSize == 0) {
				processFullWindow();
			}
			
			//manage window
			if (stepSize == maxSize) {
				//tumble
				dataWindow.clear();
			} else {
				//slide by stepSize
				for (int i = 0; i < stepSize; ++i) {
					dataWindow.remove(0);
				}
			}
		}
	}
	
	public boolean isFull() {
		return dataWindow.size() == maxSize;
	}
}
