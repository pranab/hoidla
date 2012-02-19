/*
 * hoidla: Light weight FDHS backed key value store
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

package org.hoidla.db;

import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

public class DataStore {
	private String name;
	private Path path;
	private Map<String, DataContainer> containers = new HashMap<String, DataContainer>();
	
	public DataStore(Path path) {
		this.path = path;
	}
	
	public void createContainer(String dataContainerName) {
		
	}
	
	public DataContainer getContainer(String dataContainerName) {
		DataContainer container = containers.get(dataContainerName);
		if (null == container) {
			container = new DataContainer(dataContainerName, path);
			containers.put(dataContainerName, container);
		}
		return container;
	}
	
	public void put(String dataContainerName,  DataValue dataValue ) throws IOException {
		DataContainer container = getContainer(dataContainerName);
		container.put( dataValue);
	}
	
	public DataValue get(String dataContainerName, String key) {
		DataValue dataValue = new DataValue();
		
		return dataValue;
	}
	
	
	
	public void close(String dataContainerName) {
		
	}
	
}
