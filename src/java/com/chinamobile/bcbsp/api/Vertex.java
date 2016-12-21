/**
 * CopyRight by Chinamobile
 *
 * Vertex.java
 */

package com.chinamobile.bcbsp.api;

import org.apache.hadoop.io.Writable;

/**
 * Vertex which should be extended by user.
 *
 * @param <K>
 * @param <V1>
 * @param <E>
 */
public abstract class Vertex<K, V1, E> implements VertexInterface<K, V1, E>,
    Writable {

	boolean active = true;

	public boolean isActive() {
		return active;
	}

	public void setActive(boolean active) {
		this.active = active;
	}
}
