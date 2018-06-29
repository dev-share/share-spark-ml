package com.ucloudlink.spark.pojo.recommend;

import java.io.Serializable;
import java.util.Date;

public class Level implements Serializable {
	private static final long serialVersionUID = -7272767119293149758L;
	private int userId;
	private int movieId;
	private double score;
	private Date createTime;
	public int getUserId() {
		return userId;
	}
	public void setUserId(int userId) {
		this.userId = userId;
	}
	public int getMovieId() {
		return movieId;
	}
	public void setMovieId(int movieId) {
		this.movieId = movieId;
	}
	public double getScore() {
		return score;
	}
	public void setScore(double score) {
		this.score = score;
	}
	public Date getCreateTime() {
		return createTime;
	}
	public void setCreateTime(Date createTime) {
		this.createTime = createTime;
	}
}
