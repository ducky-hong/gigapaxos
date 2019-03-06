/* Copyright (c) 2015 University of Massachusetts
 * 
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 * 
 * Initial developer(s): V. Arun */

package edu.umass.cs.utils;

import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @author arun
 * @param <K>
 * @param <V>
 *
 */
public class GCConcurrentHashMap<K, V> extends ConcurrentHashMap<K, V> {

	private static final int DEFAULT_GC_TIMEOUT = 10000;
	private static final int DEFAULT_GC_THRESHOLD_SIZE = 1024 * 64;
	private int gcThresholdSize = DEFAULT_GC_THRESHOLD_SIZE;

	final LinkedHashMap<K, Long> putTimes = new LinkedHashMap<K, Long>();
	private final GCConcurrentHashMapCallback callback;
	long gcTimeout; // milliseconds

	/**
	 * @param callback
	 * @param gcTimeout
	 */
	public GCConcurrentHashMap(GCConcurrentHashMapCallback callback,
			long gcTimeout) {
		super();
		this.callback = callback;
		this.gcTimeout = gcTimeout;
		this.minGCInterval = this.gcTimeout;
	}
	
	/**
	 * @param gcTimeout
	 */
	public GCConcurrentHashMap(long gcTimeout) {
		this(null,gcTimeout);
	}
	/**
	 * 
	 */
	private static final long serialVersionUID = 183021919212L;

	public synchronized V put(K key, V value) {
		this.putGC(key);
		V old = super.put(key, value);
		return old;
	}

	public synchronized V putIfAbsent(K key, V value) {
		this.putGC(key);
		return super.putIfAbsent(key, value);
	}

	public synchronized void putAll(Map<? extends K, ? extends V> map) {
		for (K key : map.keySet())
			this.putGC(key);
		super.putAll(map);
	}

	public synchronized V remove(Object key) {
		V value = super.remove(key);
		this.putTimes.remove(key);
		return value;
	}

	/**
	 * @param size
	 * @return {@code this}
	 */
	public GCConcurrentHashMap<K, V> setGCThresholdSize(int size) {
		this.gcThresholdSize = size;
		return this;
	}
	/**
	 * @param timeout
	 * @return {@code this}
	 */
	public GCConcurrentHashMap<K, V> setGCTimeout(long timeout) {
		this.gcTimeout = timeout;
		return this;
	}

	public synchronized boolean remove(Object key, Object value) {
		if (super.remove(key, value)) {
			this.putTimes.remove(key);
			return true;
		}
		return false;
	}

	private synchronized void putGC(K key) {
		this.putTimes.put(key, System.currentTimeMillis());
		if (this.size() > gcThresholdSize || Util.oneIn(1000))
			GC();
	}

	int numGC = 0;
	int numGCAttempts = 0;
	private long lastGCTime = 0;
	private long minGCInterval = DEFAULT_GC_TIMEOUT;

	/**
	 * @param timeout
	 */
	public synchronized void tryGC(long timeout) {
		this.GC(timeout);
	}

	private synchronized void GC() {
		this.GC(this.gcTimeout);
	}

	private synchronized void GC(long timeout) {
		if (System.currentTimeMillis() - this.lastGCTime < this.minGCInterval)
			return;
		else
			this.lastGCTime = System.currentTimeMillis();
		boolean removed = false;
		numGCAttempts++;
		for (Iterator<K> iterK = this.putTimes.keySet().iterator(); iterK
				.hasNext();) {
			K key = iterK.next();
			Long time = this.putTimes.get(key);
			if (time != null && (System.currentTimeMillis() - time > timeout)) {
				iterK.remove();
				V value = this.remove(key);
				if (value != null && this.callback!=null)
					this.callback.callbackGC(key, value);
				removed = true;
			} else
				break;
		}
		if (removed)
			numGC++;
	}
}
