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

import java.util.AbstractList;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedList;

/**
 * Base data window class
 * @author pranab
 *
 * @param <T>
 */
public abstract class DataWindow<T> {
	protected AbstractList<T> dataWindow = null;
	
	public DataWindow(boolean withSequentialAccess) {
		dataWindow = withSequentialAccess ? new LinkedList<T>() : new ArrayList<T>();
	}
	
	public void add(T obj) {
		dataWindow.add(obj);
		expire();
	}
	
	public abstract void expire();
	
	public Iterator<T> getIterator() {
		return dataWindow.iterator();
	}

	public int size() {
		return dataWindow.size();
	}
	
	public void set(int index, T obj) {
		dataWindow.set(index, obj);
	}
	
	public T get(int index) {
		return dataWindow.get(index);
	}
	
	public T getEarliest() {
		return dataWindow.get(0);
	}
	
	public T getLatest() {
		return dataWindow.get(dataWindow.size() - 1);
	}
	
	public  void processFullWindow() {
	}
	
	public void clear() {
		dataWindow.clear();
	}
	
	public abstract boolean isFull();
}
