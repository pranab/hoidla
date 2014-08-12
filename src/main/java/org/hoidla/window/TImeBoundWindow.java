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

import org.hoidla.util.TimeStamped;

public class TImeBoundWindow extends DataWindow<TimeStamped>{
	private long timeSpan;
	
	public TImeBoundWindow(long timeSpan) {
		super();
		this.timeSpan = timeSpan;
	}
	
	@Override
	public void expire() {
		// TODO Auto-generated method stub
	}

}
