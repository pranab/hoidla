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

import java.io.Serializable;
import java.util.ArrayList;


/**
 * Sliding window bounded my a max size
 * @author pranab
 *
 */
public class SizeBoundWindow<T> extends DataWindow<T> implements Serializable {
	protected int maxSize;
	private int stepSize = 1;
	private int processStepSize = 1;
	
	public SizeBoundWindow() {
	}
	
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
	 * @see org.hoidla.window.DataWindow#add(java.lang.Object)
	 */
	@Override
	public void add(T obj) {
		if (null == dataWindow) {
			dataWindow = new ArrayList<T>();
		}
		if (addFirst) {
			dataWindow.add(obj);
			++count;
			process();
			slide();
		} else {
			process();
			dataWindow.add(obj);
			++count;
			slide();
		}
	}
	

	/* (non-Javadoc)
	 * @see org.hoidla.window.DataWindow#expire()
	 */
	@Override
	public void expire() {
		//process window data
		process();
		
		//slide window
		slide();
	}
	
	/**
	 * 
	 */
	private void process() {
		//process window data
		if (count % processStepSize == 0) {
			processFullWindow();
		}
	}
	
	/**
	 * 
	 */
	private void slide() {
		//slide window
		if (dataWindow.size() > maxSize) {
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
			expired = true;
		} else {
			expired = false;
		}
	}
	
	/* (non-Javadoc)
	 * @see org.hoidla.window.DataWindow#isFull()
	 */
	public boolean isFull() {
		return dataWindow.size() == maxSize;
	}

	/**
	 * @return
	 */
	public int getMaxSize() {
		return maxSize;
	}

	/**
	 * @param maxSize
	 */
	public void setMaxSize(int maxSize) {
		this.maxSize = maxSize;
	}

	/**
	 * @return
	 */
	public int getStepSize() {
		return stepSize;
	}

	/**
	 * @param stepSize
	 */
	public void setStepSize(int stepSize) {
		this.stepSize = stepSize;
	}

	/**
	 * @return
	 */
	public int getProcessStepSize() {
		return processStepSize;
	}

	/**
	 * @param processStepSize
	 */
	public void setProcessStepSize(int processStepSize) {
		this.processStepSize = processStepSize;
	}
}
