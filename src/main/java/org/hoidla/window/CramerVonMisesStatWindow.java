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
 * CramerVonMises 2 sample stat
 * @author pranab
 *
 */
public class CramerVonMisesStatWindow extends RankSumStatWindow {

	public CramerVonMisesStatWindow() {
		super();
	}

	public CramerVonMisesStatWindow(int maxSize) {
		super(maxSize);
	}

	/* (non-Javadoc)
	 * @see org.hoidla.window.DataWindow#processFullWindow()
	 */
	public  void processFullWindow() {
		super.processFullWindow();
		
		double sum1 = 0;
		int halfSize = maxSize /2;
		for(int i = 0 ; i < halfSize; ++i) {
			double diff = positionedValues[i].getLeft() - (i + 1);
			sum1 += diff * diff;
		}
		sum1 *= halfSize;
		
		double sum2 = 0;
		for(int i = halfSize ; i < maxSize; ++i) {
			double diff = positionedValues[i].getLeft() - (i + 1);
			sum2 += diff * diff;
		}
		sum2 *= halfSize;
		
		double u = sum1 + sum2;
		stat = u / (2.0 * Math.pow(halfSize, 3)) - (4.0 * halfSize * halfSize - 1.0) / (12.0 * halfSize);
	}
	

}
