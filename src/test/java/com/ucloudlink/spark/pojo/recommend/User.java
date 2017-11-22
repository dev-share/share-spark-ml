package com.ucloudlink.spark.pojo.recommend;

import java.io.Serializable;

public class User implements Serializable {
	private static final long serialVersionUID = 1821316818024590373L;
	private int id;
	private int age;
	private char gender;
	private String duty;//职业
	private String code;//邮编
	public int getId() {
		return id;
	}
	public void setId(int id) {
		this.id = id;
	}
	public int getAge() {
		return age;
	}
	public void setAge(int age) {
		this.age = age;
	}
	public char getGender() {
		return gender;
	}
	public void setGender(char gender) {
		this.gender = gender;
	}
	public String getDuty() {
		return duty;
	}
	public void setDuty(String duty) {
		this.duty = duty;
	}
	public String getCode() {
		return code;
	}
	public void setCode(String code) {
		this.code = code;
	}
}
