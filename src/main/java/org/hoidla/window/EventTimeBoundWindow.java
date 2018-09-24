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
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.ListIterator;

import org.hoidla.util.EventTimeStamped;
import org.hoidla.util.TimeStamped;

/**
 * @author pranab
 *
 */
public class EventTimeBoundWindow extends DataWindow<EventTimeStamped>  implements Serializable {
	private List<EventTimeStamped> extendedWindow = new ArrayList<EventTimeStamped>();
	private SizeBoundIntStatsWindow statsWindow;
	private double lagStdDevMult;
	private long waterMark;
	private int waterMarkLag;
	private List<EventTimeStamped> eventTimeDataWindow = new ArrayList<EventTimeStamped>();
	private long timeSpan;
	private long timeStep = 0;
	private long processingTimeStep = -1;
	private long lastProcessedTime = -1;
	
	private static final long serialVersionUID = 8752389534053402896L;

	public EventTimeBoundWindow(long timeSpan, long timeStep, long processingTimeStep, int maxStatWindowSize, 
			double lagStdDevMult, int waterMarkLag) {
		createStatsWindow(maxStatWindowSize);
		this.lagStdDevMult = lagStdDevMult;
		this.waterMarkLag = waterMarkLag;
		this.timeSpan = timeSpan;
		this.timeStep = timeStep;
		this.processingTimeStep = processingTimeStep;
	}
	
	public void createStatsWindow(int maxStatWindowSize) {
		statsWindow = new SizeBoundIntStatsWindow(maxStatWindowSize);
	}
	
	/* (non-Javadoc)
	 * @see org.hoidla.window.DataWindow#add(java.lang.Object)
	 */
	public void add(EventTimeStamped obj) {
		extendedWindow.add(obj);
		super.add(obj);
		
		//sort by event time
		eventTimeDataWindow.clear();
		eventTimeDataWindow.addAll(extendedWindow);
		Collections.sort(eventTimeDataWindow, new Comparator<EventTimeStamped>() {
		       public int compare(EventTimeStamped o1, EventTimeStamped o2) {
		           return o1.getEventTimeStamp() < o2.getEventTimeStamp() ? -1 : (o1.getEventTimeStamp() > o2.getEventTimeStamp() ?  1 : 0);
		        }
		  });	
		
		//find out of order skew skew
		int count = 0;
		for (int i = 0; i < eventTimeDataWindow.size() -1; ++i) {
			int lag = (int)(eventTimeDataWindow.get(i).getTimeStamp() - eventTimeDataWindow.get(i+1).getTimeStamp());
			if (lag > 0) {
				statsWindow.add(lag);
			}
		}
		
		//find water mark wrt event time
		long latest = eventTimeDataWindow.get(eventTimeDataWindow.size() - 1).getEventTimeStamp();
		if (statsWindow.getCount() >= 3) {
			statsWindow.forcedProcess();
			waterMark =  latest  -  (long)(statsWindow.getMean() + lagStdDevMult * statsWindow.getStdDev());
		} else {
			waterMark =  latest -  waterMarkLag;
		}
		
		long timespandEnd = eventTimeDataWindow.get(0).getEventTimeStamp() + timeSpan;
		if ( timespandEnd < waterMark) {
			//only events above water mark and enough data for processing
			dataWindow.clear();
			for (EventTimeStamped event : eventTimeDataWindow) {
				if (event.getEventTimeStamp() < timespandEnd) {
					dataWindow.add(event);
				}
			}
			
			expire();
		}
	}
	
	@Override
	public void expire() {
		//process data
		boolean processed = false;
		long latest = dataWindow.get(dataWindow.size() - 1).getEventTimeStamp();
		if (processingTimeStep > 0 && lastProcessedTime > 0) {
			if (latest - lastProcessedTime > processingTimeStep) {
				processFullWindow();
				lastProcessedTime = latest;
				processed = true;
			}
		} else {
			processFullWindow();
			lastProcessedTime = latest;
			processed = true;
		}
		
		//expire data 
		long earliestRetained = latest - timeSpan + timeStep;
		ListIterator<EventTimeStamped> iter =  dataWindow.listIterator();
		while (iter.hasNext()) {
			if (iter.next().getEventTimeStamp() < earliestRetained) {
				iter.remove();
			}
		}
		
	}

	@Override
	public boolean isFull() {
		// TODO Auto-generated method stub
		return false;
	}

}
