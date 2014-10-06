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
 * @author pranab
 *
 * @param <T>
 */
public class BiasedReservoirWindow<T> extends DataWindow<T> {
	private int maxSize;
	
	/**
	 * @param maxSize
	 */
	public BiasedReservoirWindow(int maxSize) {
		super(false);
		this.maxSize = maxSize;
	}

	@Override
	public void add(T obj) {
		int elementToReplace = -1;
		int curSize = size();
		if (curSize == maxSize) {
			//always replace
			elementToReplace = (int)(Math.random() * curSize);
		} else {
			//may or may not replace
			if (curSize > 0 && Math.random() < (curSize / (double)maxSize)) {
				elementToReplace = (int)(Math.random() * curSize);
			}
		}
		if (elementToReplace >= 0) {
			set(elementToReplace, obj);
		} else {
			super.add(obj);
		}
	}
	
	@Override
	public void expire() {
	}

	public boolean isFull() {
		return dataWindow.size() == maxSize;
	}

}
