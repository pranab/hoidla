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

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;


public class DataManager  {
	private String dataManagerPath;
	private Configuration config;
	private Map<String, DataStore> stores = new HashMap<String, DataStore>();
	private static DataManager dataManager;
	
	public static DataManager instance() {
		if (null == dataManager) {
			dataManager = new DataManager();
		}
		return dataManager;
	}
	
	public void initialize(String dataManagerPath, Configuration config) {
		this.dataManagerPath = dataManagerPath;
		this.config = config;
	}
	
	public DataStore createDataStore(String dataStoreName) {
		Path storePath = new Path(dataManagerPath, dataStoreName);
		DataStore dataStore = new DataStore(storePath);
		stores.put(dataStoreName, dataStore);
		return dataStore;
	}
	
	public DataStore getDataStore(String dataStoreName) {
		DataStore dataStore = stores.get(dataStoreName);
		if (null == dataStore) {
			dataStore = createDataStore( dataStoreName);
		}
		return dataStore;
	}
	
	public DataContainer getContainer(String store, String container) {
		DataContainer dataContainer = null;
		
		return dataContainer;
	}

	public Configuration getConfig() {
		return config;
	}
	
}
