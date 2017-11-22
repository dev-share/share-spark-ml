package com.ucloudlink.spark.pojo.cluster;

import java.io.Serializable;

public class Genre implements Serializable {
	private static final long serialVersionUID = -7900353655940143234L;
	private String material;
	private int index;
	public String getMaterial() {
		return material;
	}
	public void setMaterial(String material) {
		this.material = material;
	}
	public int getIndex() {
		return index;
	}
	public void setIndex(int index) {
		this.index = index;
	}
}
