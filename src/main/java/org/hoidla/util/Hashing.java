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

import java.io.UnsupportedEncodingException;

/**
 * Different hashing algorithms
 * @author pranab
 *
 */
public class Hashing {

	/**
	 * @author pranab
	 *
	 */
	public static class FowlerNashVo {
		private static final int INIT  =  0xcbf29de4;
		private static final int PRIME  = 997;
		private int hash;
		private byte[] data;
		
		public void initialize(byte[] data) {
			hash = INIT;
			this.data = data;
		}
		
		/**
		 * Mutiple hash on same value
		 * @return
		 */
		public int next() {
			for (int i = 0; i < data.length; ++i) {
				hash ^= data[i];
				hash *= PRIME;
			}
			hash &= 0xefffffff;
			return hash;
		}
	}
	
	/**
	 * Murmur hash 2 as per www.getopt.org
	 * @author pranab
	 *
	 */
	public static class MurmurHash {
		
		  public int hash(byte[] data, int seed) {
			  int m = 0x5bd1e995;
			  int r = 24;

			  int len = data.length;
			  int h = seed ^ len;
			  int lenFour = len >> 2;

		    	for (int i = 0;  i < lenFour;  i++) {
		    		int iFour = i << 2;
		    		int k = data[iFour + 3];
		    		k = k << 8;
		    		k = k | (data[iFour + 2] & 0xff);
		    		k = k << 8;
		    		k = k | (data[iFour + 1] & 0xff);
		    		k = k << 8;
		    		k = k | (data[iFour + 0] & 0xff);
		    		k *= m;
		    		k ^= k >>> r;
		    		k *= m;
		    		h *= m;
		    		h ^= k;
		    	}

		    	int lenM = lenFour << 2;
		    	int left = len - lenM;

		    	if (left != 0) {
		    		if (left >= 3) {
		    			h ^= (int) data[len - 3] << 16;
		    		}
		    		if (left >= 2) {
		    			h ^= (int) data[len - 2] << 8;
		    		}
		    		if (left >= 1) {
		    			h ^= (int) data[len - 1];
		    		}

		    		h *= m;
		    }

		    h ^= h >>> 13;
		    h *= m;
		    h ^= h >>> 15;

		    return h;
		  }	
	}
	
	/**
	 * Family of xor based hash function with bounded hash values
	 * @author pranab
	 *
	 */
	public static class MultiHashFamily {
		private int numHash;
		private int hashValueMax;
		private int[] a;
		private int[] b;
		private final  int prime = 1000099;

		/**
		 * @param numHash
		 * @param hashValueMax
		 */
		public MultiHashFamily(int numHash, int hashValueMax) {
			this(numHash);
			this.hashValueMax = hashValueMax;
		}
		
		/**
		 * @param numHash
		 */
		public MultiHashFamily(int numHash) {
			this.numHash = numHash;
			a = new int[numHash];
			b = new int[numHash];
			
			for (int i = 0; i < numHash; ++i) {
				a[i] = (int)(Math.random() * prime);
				b[i] = (int)(Math.random() * prime);
			}
		}

		/**
		 * @param data
		 * @param hashFun
		 * @return
		 */
		public int hash(Object data, int hashFun) {
			int hashCode = 0;
			if (data instanceof String) {
				try {
					byte[] bytesData = ((String)data).getBytes("utf-8");
					hashCode = hash(bytesData, hashFun);
				} catch (UnsupportedEncodingException e) {
					throw new IllegalArgumentException("failed to decode string into byte array" + e.getMessage());
				}
			} else if (data instanceof Integer) {
				int intData = (Integer)data;
				hashCode = hash(intData, hashFun);
			}
			
			return hashCode;
		}

		/**
		 * @param data
		 * @param hashFun
		 * @return
		 */
		public int hash(byte[] data, int hashFun) {
			if (hashFun <0 || hashFun >= numHash) {
				throw new IllegalArgumentException("invalid hash function index " + hashFun);
			}
			int hashCode;
			int accum = 0;
			for (int i =0; i < data.length; ++i) {
				accum += data[i] * a[hashFun];
			}
			accum += b[hashFun];
			accum &= 0x7fffffff;
			hashCode =  accum % prime;
			if (hashValueMax > 0) {
				hashCode= hashCode % hashValueMax;
			}
			return hashCode;
		}

		/**
		 * @param data
		 * @param hashFun
		 * @return
		 */
		public int hash(int data, int hashFun) {
			if (hashFun <0 || hashFun >= numHash) {
				throw new IllegalArgumentException("invalid hash function index " + hashFun);
			}
			int hashCode  = data *  a[hashFun] + b[hashFun];
			hashCode &= 0xefffffff;
			hashCode =  hashCode % prime;
			if (hashValueMax > 0) {
				hashCode= hashCode % hashValueMax;
			}
			return hashCode;
		}
		
		/**
		 * @param data
		 * @param hashFun
		 * @return
		 */
		public int hash(String data, int hashFun) {
			if (hashFun <0 || hashFun >= numHash) {
				throw new IllegalArgumentException("invalid hash function index " + hashFun);
			}
			int hashCode  = data.hashCode() *  a[hashFun] + b[hashFun];
			hashCode &= 0xefffffff;
			hashCode =  hashCode % prime;
			if (hashValueMax > 0) {
				hashCode= hashCode % hashValueMax;
			}
			return hashCode;
		}
		
	}
	
}
