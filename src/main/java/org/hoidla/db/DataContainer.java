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
import java.util.Arrays;
import java.util.Comparator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.util.ReflectionUtils;
import org.hoidla.serde.SerDes;

public class DataContainer {
		private String name;
		private Path storePath;
		private SequenceFile.Writer writer;
		
		public DataContainer(String name, Path storePath) {
			this.name = name;
			this.storePath = storePath;
		}

		public void  get( DataValue dataValue)  throws IOException {
			Path filesPath = new  Path(storePath, "*");
			FileSystem fileSys = storePath.getFileSystem(DataManager.instance().getConfig());
			FileStatus[] fileStats = fileSys.globStatus(filesPath);
			Configuration config = DataManager.instance().getConfig();
			
			if (fileStats != null && fileStats.length > 0) {
				KeyWritable keyWr = null;
				ValueWritable valueWr = null;
				SequenceFile.Reader reader = null;
				
	            if ( fileStats.length > 1 ) {
	            	Arrays.sort(fileStats, new FilelatestComparator()    );
	            }
	
				for (FileStatus fileStat : fileStats) {
					if (fileStat.getPath().getName().startsWith(name)) {
						reader = new SequenceFile.Reader(fileSys, fileStat.getPath(), config);
	                	keyWr = (KeyWritable) ReflectionUtils.newInstance(reader.getKeyClass(), config);
	                	valueWr = (ValueWritable) ReflectionUtils.newInstance(reader.getValueClass(), config);	
	                	
	                	while(reader.next(keyWr, valueWr)) {
	                		if (keyWr.getKey().equals(dataValue.getKey())) {
	                			byte[] bytes = valueWr.getValue();
	                			if (dataValue.getDataType() == DataValue.TYPE_BYTE_ARRAY) {
	                				dataValue.setValue(bytes);
	                			} else {
	                				SerDes sd = DataManager.instance().getSerDes(dataValue.getDataType());
	                				if (null == sd){
	                					throw new IOException("Read failed , no serde found for data type " + dataValue.getDataType());
	                				}
	                				Object value = sd.deserialize(bytes);
	                				dataValue.setValue(value);
	                			}
	                			break;
	                		}
		                	keyWr = (KeyWritable) ReflectionUtils.newInstance(reader.getKeyClass(), config);
		                	valueWr = (ValueWritable) ReflectionUtils.newInstance(reader.getValueClass(), config);	
	                		
	                	}
                    	IOUtils.closeStream(reader);
                    	reader = null;
                		if (null != dataValue.getValue()) {
                			break;
                		}
					} 
				}
			}
		}
		
		public void put( DataValue dataValue) throws IOException {
			byte[] bytes = null;
			getWriter();
			
			//key and value
			KeyWritable keyWr = new KeyWritable(dataValue.getKey(), System.currentTimeMillis(),  KeyWritable.UPDATE_OPCODE);
			if (dataValue.getDataType() == DataValue.TYPE_BYTE_ARRAY) {
				bytes = (byte[])dataValue.getValue();
			} else {
				SerDes sd = DataManager.instance().getSerDes(dataValue.getDataType());
				if (null == sd){
					throw new IOException("Write failed , no serde found for data type " + dataValue.getDataType());
				}
				bytes = sd.serialize(dataValue.getValue());
			}
			ValueWritable valWr = new ValueWritable(bytes, dataValue.getComment());
			
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
				throw new IOException("Failed to create container file ", e);
			}
			
			return writer;
		}
		
	    private static class FilelatestComparator implements Comparator<FileStatus> {
	        public int compare(FileStatus file1, FileStatus file2) {
	        	return Long.valueOf(file2.getModificationTime()).compareTo(file1.getModificationTime()) ;
	        }
	    }	    
		
}
