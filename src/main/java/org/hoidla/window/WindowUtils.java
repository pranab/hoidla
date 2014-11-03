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

import org.apache.commons.math3.stat.StatUtils;
import org.hoidla.util.ExplicitlyTimeStamped;
import org.hoidla.util.ImplicitlyTimeStamped;
import org.hoidla.util.TimeStamped;

/**
 * @author pranab
 *
 */
public  class WindowUtils {

	/**
	 * Converts size bound window to array of doubles
	 * @param window
	 * @return array of doubles
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
	 * Converts time bound window to array of bytes
	 * @param window
	 * @return array of doubles
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
	
	/**
	 * Mean
	 * @param window
	 * @return
	 */
	public static <T> double getMean(SizeBoundWindow<T> window) {
		double[] data = getDoubleArray1(window);
		return StatUtils.mean(data);
	}
	
	/**
	 * Std dev
	 * @param window
	 * @return
	 */
	public static <T> double getStdDev(SizeBoundWindow<T> window) {
		double[] data = getDoubleArray1(window);
		return Math.sqrt(StatUtils.variance(data));
	}

	/**
	 * Median
	 * @param window
	 * @return
	 */
	public static <T> double getMedian(SizeBoundWindow<T> window) {
		double[] data = getDoubleArray1(window);
		return StatUtils.percentile(data, 50);
	}

	/**
	 * Mean
	 * @param window
	 * @return
	 */
	public static <T> double getMean(TimeBoundWindow window) {
		double[] data = getDoubleArray1(window);
		return StatUtils.mean(data);
	}
	
	/**
	 * Std dev
	 * @param window
	 * @return
	 */
	public static <T> double getStdDev(TimeBoundWindow window) {
		double[] data = getDoubleArray1(window);
		return Math.sqrt(StatUtils.variance(data));
	}

	/**
	 * Median
	 * @param window
	 * @return
	 */
	public static <T> double getMedian(TimeBoundWindow window) {
		double[] data = getDoubleArray1(window);
		return StatUtils.percentile(data, 50);
	}
	
	/**
	 * Mean
	 * @param window
	 * @return
	 */
	public static double getMean(double[] data) {
		return StatUtils.mean(data);
	}
	
	/**
	 * Std dev
	 * @param window
	 * @return
	 */
	public static double getStdDev(double[] data) {
		return Math.sqrt(StatUtils.variance(data));
	}

	/**
	 * Median
	 * @param window
	 * @return
	 */
	public static double getMedian(double[] data) {
		return StatUtils.percentile(data, 50);
	}
}
