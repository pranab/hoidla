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

import java.util.HashMap;
import java.util.Map;

/**
 * Ngram counting window
 * @author pranab
 *
 */
public class NgramCounterWindow extends SizeBoundSymbolWindow {
	/**
	 * 
	 */
	private static final long serialVersionUID = 4567074911259665894L;
	private boolean isFull = false;
	private int ngramSize;
	private Map<String[], Integer> counts = null;
	private String earliest = null;

	public NgramCounterWindow(int maxSize, int ngramSize) {
		super(maxSize);
		this.ngramSize = ngramSize;
	}
	
	/* (non-Javadoc)
	 * @see org.hoidla.window.DataWindow#add(java.lang.Object)
	 */
	public void add(String obj) {
		if (dataWindow.size() == maxSize) {
			earliest = dataWindow.get(0);
		}
		super.add(obj);
	}
	
	/* (non-Javadoc)
	 * @see org.hoidla.window.DataWindow#processFullWindow()
	 */
	public  void processFullWindow() {
		counts = new HashMap<String[], Integer>();
		if (!isFull) {
			//first time full, return all counts
			for (int i = 0; i < maxSize - ngramSize; ++i) {
				String[] ngram = new String[ngramSize];
				for(int j = 0; j < ngramSize; ++j) {
					ngram[j] = dataWindow.get(i + j);
				}
				Integer count = counts.get(ngram);
				if (null == count) {
					counts.put(ngram, 1);
				} else {
					counts.put(ngram, count+1);
				}
			} 
			isFull = true;
		} else {
			//dropped ngram
			String[] ngram = new String[ngramSize];
			ngram[0] = earliest;
			for(int j = 1; j < ngramSize; ++j) {
				ngram[j] = dataWindow.get(j - 1);
			}
			counts.put(ngram, -1);
			
			//new ngram
			ngram = new String[ngramSize];
			for(int j = 0, k = maxSize - ngramSize; j < ngramSize; ++j, ++k) {
				ngram[j] = dataWindow.get(k);
			}
			counts.put(ngram, 1);
		}
	}	
	

	/**
	 * @return
	 */
	public Map<String[], Integer> getNgramCounts() {
		return counts;
	}
}
