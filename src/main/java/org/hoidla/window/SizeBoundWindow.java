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

import java.util.Iterator;
import java.util.LinkedList;

/**
 * @author pranab
 *
 */
public class SizeBoundWindow<T> extends DataWindow<T>{
	private int maxSize;
	private int stepSize = 1;
	
	public SizeBoundWindow(int maxSize) {
		super(true);
		this.maxSize = maxSize;
	}
	
	public SizeBoundWindow(int maxSize, int stepSize) {
		this(maxSize);
		this.stepSize = stepSize;
	}
	
	public void expire() {
		if (dataWindow.size() > maxSize) {
			if (stepSize > 1) {
				processFullWindow();
			}
			if (stepSize == maxSize) {
				dataWindow.clear();
			} else {
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
