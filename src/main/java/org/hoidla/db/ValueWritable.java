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

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

public class ValueWritable  implements Writable {
	private BytesWritable value = new BytesWritable();
	private Text comment = new Text();

	public ValueWritable(){
	}
	
	public ValueWritable(byte[] value, String comment){
		this.value.set(value, 0, value.length);
		this.comment.set(comment);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		value.readFields(in);
		comment.readFields(in);
	}

	@Override
	public void write(DataOutput out) throws IOException {
		value.write(out);
		comment.write(out);
	}

}
