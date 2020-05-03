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

public class SizeBoundSubSamplerWindow extends SizeBoundWindow<Double> {
	private boolean useAverage;
	private double sample;
	
	/**
	 * @param maxSize
	 * @param stepSize
	 * @param processStepSize
	 * @param useAverage
	 */
	public SizeBoundSubSamplerWindow(int maxSize, int stepSize, int processStepSize, boolean useAverage) {
		super(maxSize, stepSize, processStepSize);
		this.useAverage = useAverage;
	}

	
	/**
	 * @param maxSize
	 * @param stepSize
	 * @param processStepSize
	 */
	public SizeBoundSubSamplerWindow(int maxSize, int stepSize, int processStepSize) {
		super(maxSize, stepSize, processStepSize);
	}

	/* (non-Javadoc)
	* @see org.hoidla.window.DataWindow#processFullWindow()
	*/
	public  void processFullWindow() {
		if (useAverage) {
			double sum = 0;
			for (double val : dataWindow) {
				sum += val;
			}
			sample = sum / maxSize;
		} else {
			sample = dataWindow.get(maxSize-1);
		}
	}
	
	/**
	 * @return
	 */
	public double getSample() {
		return sample;
	}

}
