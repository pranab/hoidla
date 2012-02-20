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

package org.hoidla.serde;

import java.io.IOException;
import java.io.UnsupportedEncodingException;

public class StringSerDes implements SerDes {

	@Override
	public byte[] serialize(Object value) throws IOException{
		byte[] barr = null;
		try {
			barr =  ((String)value).getBytes("utf-8");
		} catch (UnsupportedEncodingException e) {
			throw new IOException("Failed to serialize string", e);
		}
		
		return barr;
	}

	@Override
	public Object deserialize(byte[] bytes) throws IOException{
		return new String(bytes, "utf-8");
	}

}
