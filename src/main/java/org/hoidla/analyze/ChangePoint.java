/*
 * hoidla: various algorithms for sequence data solutions
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

package org.hoidla.analyze;

import org.chombo.util.Pair;

/**
 * @author pranab
 *
 */
public class ChangePoint extends Pair<Integer, Double> implements Comparable {

	public ChangePoint(Integer index, Double disc) {
		super(index, disc);
	}
	
	public Integer getIndex() {
		return left;
	}
	
	public Double getDiscrepancy() {
		return right;
	}

	@Override
	public int compareTo(Object obj) {
		ChangePoint that = (ChangePoint)obj;
		return (int)(that.getDiscrepancy() - this.getDiscrepancy());
	}

}
