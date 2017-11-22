package com.glocalme.css.spark.vsim.pojo;

import java.io.Serializable;
import java.util.Date;

import com.glocalme.css.spark.util.DateUtil;

public class UserSleepHistory implements Serializable {
	private static final long serialVersionUID = -2667742155600942370L;
	private String id;
	private String user_code;
	private long imsi;
	private String imei;
	private int op_type;
	private long op_time;
	private String group_id;
	private int create_date_year;
	private int create_date_month;
	private int create_date_day;
	private int create_date_hour;
	private int create_date_minute;
	public String getId() {
		return id;
	}
	public void setId(String id) {
		this.id = id;
	}
	public String getUser_code() {
		return user_code;
	}
	public void setUser_code(String user_code) {
		this.user_code = user_code;
	}
	public long getImsi() {
		return imsi;
	}
	public void setImsi(long imsi) {
		this.imsi = imsi;
	}
	public String getImei() {
		return imei;
	}
	public void setImei(String imei) {
		this.imei = imei;
	}
	public int getOp_type() {
		return op_type;
	}
	public void setOp_type(int op_type) {
		this.op_type = op_type;
	}
	public long getOp_time() {
		return op_time;
	}
	public void setOp_time(long op_time) {
		this.op_time = op_time;
	}
	public String getGroup_id() {
		return group_id;
	}
	public void setGroup_id(String group_id) {
		this.group_id = group_id;
	}
	public int getCreate_date_year() {
		return create_date_year;
	}
	public void setCreate_date_year(int create_date_year) {
		this.create_date_year = create_date_year;
	}
	public int getCreate_date_month() {
		return create_date_month;
	}
	public void setCreate_date_month(int create_date_month) {
		this.create_date_month = create_date_month;
	}
	public int getCreate_date_day() {
		return create_date_day;
	}
	public void setCreate_date_day(int create_date_day) {
		this.create_date_day = create_date_day;
	}
	public int getCreate_date_hour() {
		return create_date_hour;
	}
	public void setCreate_date_hour(int create_date_hour) {
		this.create_date_hour = create_date_hour;
	}
	public int getCreate_date_minute() {
		return create_date_minute;
	}
	public void setCreate_date_minute(int create_date_minute) {
		this.create_date_minute = create_date_minute;
	}
	public String getOptTime(){
		String optTime = null;
		if(this.getOp_time()>0){
			optTime = DateUtil.formatDateHMTimeStr(new Date(this.getOp_time()));
		}
		return optTime;
	}
}
