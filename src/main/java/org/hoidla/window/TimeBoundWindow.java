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

import java.io.Serializable;
import java.util.ListIterator;

import org.hoidla.util.TimeStamped;

/**
 * Time bound window
 * @author pranab
 *
 */
public class TimeBoundWindow extends DataWindow<TimeStamped> implements Serializable {
	private long timeSpan;
	private long timeStep = 0;
	private long processingTimeStep = -1;
	private long lastProcessedTime = -1;
	
	/**
	 * @param timeSpan
	 */
	public TimeBoundWindow(long timeSpan) {
		super(true);
		this.timeSpan = timeSpan;
	}
	
	/**
	 * @param timeSpan
	 * @param timeStep
	 */
	public TimeBoundWindow(long timeSpan, long timeStep) {
		this(timeSpan);
		this.timeStep = timeStep;
	}

	/**
	 * @param timeSpan
	 * @param timeStep
	 */
	public TimeBoundWindow(long timeSpan, long timeStep, long processingTimeStep) {
		this(timeSpan, timeStep);
		this.processingTimeStep = processingTimeStep;
	}

	/* (non-Javadoc)
	 * @see org.hoidla.window.DataWindow#expire()
	 */
	@Override
	public void expire() {
		//slide by timeStep duration
		TimeStamped earliest = getEarliest();
		TimeStamped latest = getLatest();
		if ((latest.getTimeStamp() - earliest.getTimeStamp()) > timeSpan) {
			//System.out.println("in expire earliest: " + earliest.getTimeStamp() + 
			//		" latest: " + latest.getTimeStamp() + " window size: " + size());
			
			//process window
			processFullWindowHelper(earliest, latest);
			
			//manage window
			long earliestRetained = latest.getTimeStamp() - timeSpan + timeStep;
			ListIterator<TimeStamped> iter =  dataWindow.listIterator();
			int numRemoved = 0;
			while (iter.hasNext()) {
				if (iter.next().getTimeStamp() < earliestRetained) {
					iter.remove();
					++numRemoved;
				}
			}
			//System.out.println("in expire num of items removed: " + numRemoved + " window size: " + size());
		}
	}
	
	/**
	 * @param earliest
	 * @param latest
	 * @return
	 */
	private boolean processFullWindowHelper(TimeStamped earliest, TimeStamped latest) {
		boolean processed = false;
		if (processingTimeStep > 0 && lastProcessedTime > 0) {
			if (latest.getTimeStamp() - lastProcessedTime > processingTimeStep) {
				processFullWindow();
				lastProcessedTime = latest.getTimeStamp();
				processed = true;
			}
		} else {
			//System.out.println("calling processFullWindow");
			processFullWindow();
			lastProcessedTime = latest.getTimeStamp();
			processed = true;
		}
		
		return processed;
	}

	/* (non-Javadoc)
	 * @see org.hoidla.window.DataWindow#isFull()
	 */
	public boolean isFull() {
		TimeStamped earliest = getEarliest();
		TimeStamped latest = getLatest();
		return (latest.getTimeStamp() - earliest.getTimeStamp()) > (0.95 * timeSpan);
	}
}
