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

package org.hoidla.stream;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.hoidla.util.Expirer;
import org.hoidla.util.Hashing;
import org.hoidla.util.ObjectCounter;
import org.hoidla.util.SequencedObjectCounter;
import org.hoidla.util.SimpleObjectCounter;

/**
 * Frequent distribution by Count Sketch
 * @author pranab
 *
 */
public class CountSketch implements FrequentItems.FrequencyDistribution {
	//sketch
	protected int width;
	protected int depth;
	protected ObjectCounter[][] sketch;
	protected Expirer expirer;

	private Hashing.MultiHashFamily hashFamily;
	private Hashing.MultiHashFamily multiplierHashFamily;

	/** 
	 * Constructor based on error bounds
	 * @param errorLimit
	 * @param errorProbLimit
	 */
	public CountSketch(double errorLimit, double errorProbLimit) {
		width = (int)Math.round(2.0 / errorLimit);
		depth = (int)Math.round(Math.log(errorProbLimit));
		initialize(width, depth);
	}	
	
	public CountSketch(double errorLimit, double errorProbLimit, Expirer expirer) {
		this.expirer = expirer;
		width = (int)Math.round(2.0 / errorLimit);
		depth = (int)Math.round(Math.log(errorProbLimit));
		initialize(width, depth);
	}	

	/**
	 * Constructor  base of number of hash functions and hash value range
	 * @param width
	 * @param depth
	 */
	public CountSketch(int width, int depth) {
		initialize(width, depth);
	}

	/**
	 * @param width
	 * @param depth
	 */
	public void initialize(int width, int depth) {
		hashFamily = new Hashing.MultiHashFamily(width, depth);
		multiplierHashFamily = new Hashing.MultiHashFamily(width);
		for (int i = 0; i < depth; ++i) {
			for (int j = 0; j < width; ++j) {
				sketch[i][j] = expirer == null ? new SimpleObjectCounter() :  new SequencedObjectCounter();
			}
		}
	}
	
	@Override
	public void add(Object value) {
		for (int d = 0; d < depth; ++d) {
			int w = hashFamily.hash(value,  d);
			int count = (multiplierHashFamily.hash(value,  d) % 2) * 2 - 1;
			ObjectCounter counter = sketch[d][w];
			counter.change(count);
		}
	}

	@Override
	public void add(Object value, long sequence) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public int getDistr(Object value) {
		int median = 0;
		List<Integer> hashCounts = new ArrayList<Integer>();
		for (int d = 0; d < depth; ++d) {
			int w = hashFamily.hash(value,  d);
			int mult = (multiplierHashFamily.hash(value,  d) % 2) * 2 - 1;
			int thisCount = sketch[d][w].getCount();
			hashCounts.add(thisCount * mult );
		}			
		Collections.sort(hashCounts);
		int mid = hashCounts.size() / 2;
		if (hashCounts.size() % 2 == 1) {
			median = hashCounts.get(mid);
		} else {
			median = (hashCounts.get(mid) + hashCounts.get(mid + 1)) / 2;
		}
		
		return median;
	}

}
