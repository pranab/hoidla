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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

public class KeyWritable implements Writable {
	private  Text key = new Text();
	private LongWritable version = new LongWritable() ;
	private Text opCode = new  Text();
	public static final String UPDATE_OPCODE = "U";
	public static final String DELETE_OPCODE = "D";
	
	
	public KeyWritable() {
	}
	
	public KeyWritable(String key, long version, String opCode) {
		this.key.set(key);
		this.version.set(version);
		this.opCode.set(opCode);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		key.readFields(in);
		version.readFields(in);
		opCode.readFields(in);
		
	}
	@Override
	public void write(DataOutput out) throws IOException {
		key.write(out);
		version.write(out);
		opCode.write(out);
		
	}
	
	public String getKey() {
		return key.toString();
	}

}
