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

import org.hoidla.util.ExplicitlyTimeStamped;
import org.hoidla.util.ImplicitlyTimeStamped;
import org.hoidla.util.TimeStamped;

/**
 * @author pranab
 *
 */
public  class WindowUtils {

	/**
	 * @param window
	 * @return
	 */
	public static <T> double[] getDoubleArray1(SizeBoundWindow<T> window) {
		double[] data = new double[window.size()];
		for (int i = 0; i < window.size(); ++i) {
			T item  = window.get(i);
			if (item instanceof Integer) {
				data[i] = (Integer)item;
			} else if (item instanceof Long) {
				data[i] = (Long)item;
			} else if (item instanceof Double) {
				data[i] = (Double)item;
			}
		}
		return data;
	}

	/**
	 * @param window
	 * @return
	 */
	public static  double[] getDoubleArray1(TimeBoundWindow window) {
		double[] data = new double[window.size()];
		for (int i = 0; i < window.size(); ++i) {
			TimeStamped timeStapmed  = window.get(i);
			Object value = null;
			if (timeStapmed instanceof ExplicitlyTimeStamped) {
				value = ((ExplicitlyTimeStamped)timeStapmed).getValue();
			} else if (timeStapmed instanceof ExplicitlyTimeStamped) {
				value = ((ImplicitlyTimeStamped)timeStapmed).getValue();
			} 
			
			//value
			if (value instanceof Integer) {
				data[i] = (Integer)value;
			} else if (value instanceof Long) {
				data[i] = (Long)value;
			} else if (value instanceof Double) {
				data[i] = (Double)value;
			}
		}
		return data;
	}
}
