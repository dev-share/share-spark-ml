package com.ucloudlink.spark.pojo.recommend;

import java.io.Serializable;
import java.util.Date;

public class Movie implements Serializable {
	private static final long serialVersionUID = 3140957050590671247L;
	private int id;
	private String title;
	private Date date;
	private String url;
	public int getId() {
		return id;
	}
	public void setId(int id) {
		this.id = id;
	}
	public String getTitle() {
		return title;
	}
	public void setTitle(String title) {
		this.title = title;
	}
	public Date getDate() {
		return date;
	}
	public void setDate(Date date) {
		this.date = date;
	}
	public String getUrl() {
		return url;
	}
	public void setUrl(String url) {
		this.url = url;
	}
}
