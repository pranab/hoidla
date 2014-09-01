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

package org.hoidla.util;

/**
 * Manages daily schedule defined as list of minutes after midnight
 * @author pranab
 *
 */
public class DailySchedule {
	private int resolutionSec;
	private int[] secondsAfter;
	private long lastTrigger;
	
	public DailySchedule(int resolutionSec, int... minutesAfter) {
		this.resolutionSec = resolutionSec;
		this.secondsAfter = minutesAfter;
		for (int i = 0; i < secondsAfter.length; ++i) {
			secondsAfter[i] *= 60;
		}
	}
	
	/**
	 * @return
	 */
	public boolean shouldTrigger() {
		long now = System.currentTimeMillis() / 1000;
		boolean trigger = false;
		
		for (int secondAfter :  secondsAfter) {
			long secAfterSchedule = now % secondAfter;
			if (secAfterSchedule < resolutionSec) {
				long currentTrigger = now  - secAfterSchedule;
				trigger = currentTrigger > lastTrigger;
				if (trigger) {
					lastTrigger = currentTrigger;
					break;
				}
			}
		}
		return trigger;
	}
}
