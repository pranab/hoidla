/*
 * hoidla: various streaming algorithms for Big Data solutions
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

/**
 * Time bound window but aligned. For example, id time span is 1 hour, window boundaries will
 * be aligned with hours
 * @author pranab
 *
 */
public class TimeBoundAlignedWindow extends TimeBoundWindow {
	private long begTime = -1;
	private long endTime = -1;
	
	/**
	 * @param timeSpan
	 */
	public TimeBoundAlignedWindow(long timeSpan) {
		super(timeSpan);
		timeStep = processingTimeStep = timeSpan;
		
	}
	
	/* (non-Javadoc)
	 * @see org.hoidla.window.DataWindow#expire()
	 */
	@Override
	public void expire() {
		TimeStamped obj = getLatest();
		long newBegTime = (obj.getTimeStamp() / timeSpan) * timeSpan;
		long newEndTime = newBegTime + timeSpan;
		if (-1 == begTime) {
			//initial
			begTime = newBegTime;
			endTime = newEndTime;
		} else if (newBegTime > begTime) {
			//new window
			processFullWindow();
			clear();
			begTime = newBegTime;
			endTime = newEndTime;
		} else {
			//current window
		}
	}
}
