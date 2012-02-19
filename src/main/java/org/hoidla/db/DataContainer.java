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

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;

public class DataContainer {
		private String name;
		private Path storePath;
		private SequenceFile.Writer writer;
		
		
		public DataContainer(String name, Path storePath) {
			super();
			this.name = name;
			this.storePath = storePath;
		}

		public DataValue  get(String key)  {
			DataValue dataValue  = null;
			
			return dataValue;
		}
		
		public void put( DataValue dataValue) throws IOException {
			getWriter();
			KeyWritable keyWr = new KeyWritable(dataValue.getKey(), System.currentTimeMillis(),  KeyWritable.UPDATE_OPCODE);
			ValueWritable valWr = new ValueWritable(dataValue.getValue(), dataValue.getComment());
			writer.append(keyWr, valWr);
		} 
		
		public void delete(String key) {
			
		}
		
		private SequenceFile.Writer getWriter() throws IOException {
			try {
				if (null == writer) {
					String contPathName = name + "_" + System.currentTimeMillis();
					Path contPath = new Path(storePath, contPathName);
					FileSystem fileSys = contPath.getFileSystem(DataManager.instance().getConfig());
					Class keyClazz = Class.forName("org.hoidla.db.KeyWritable");
					Class valClazz = Class.forName("org.hoidla.db.ValueWritable");

					writer = SequenceFile.createWriter(fileSys, DataManager.instance().getConfig(), contPath, 
							keyClazz, valClazz);
				} 
			} catch (ClassNotFoundException e) {
				throw new IOException("", e);
			}
			
			return writer;
		}
		
}
