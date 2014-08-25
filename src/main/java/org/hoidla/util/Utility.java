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

import java.util.List;

/**
 * @author pranab
 *
 */
public  class Utility {

    /**
     * @param list
     * @return
     */
    public static <T> String join(List<T> list, String delim) {
    	StringBuilder stBld = new StringBuilder();
    	for (T obj : list) {
    		stBld.append(obj).append(delim);
    	}
    	return stBld.length() > 0 ? stBld.substring(0, stBld.length() -1) : "";
    }
  
    /**
     * @param list
     * @return
     */
    public static <T> String join(List<T> list) {
    	return join(list, ",");
    }
    
    /**
     * @param arr
     * @param delim
     * @return
     */
    public static <T> String join(T[] arr, String delim) {
    	StringBuilder stBld = new StringBuilder();
    	for (T obj : arr) {
    		stBld.append(obj).append(delim);
    	}
    	return stBld.length() > 0 ? stBld.substring(0, stBld.length() -1) : "";
    }

    /**
     * @param arr
     * @return
     */
    public static <T> String join(T[] arr) {
    	return join(arr, ",");
    }

}
