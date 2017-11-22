package com.ucloudlink.spark.pojo.regression;

import java.io.Serializable;
import java.util.Date;

public class BikeHour implements Serializable {
	private static final long serialVersionUID = 2442156646326360220L;
	private int instant;
	private Date dteday;
	private int season;//(1:springer, 2:summer, 3:fall, 4:winter)
	private int yr;//(0: 2011, 1:2012)
	private int mnth;//( 1 ~ 12)
	private int hr;//( 0 ~ 23)
	private int holiday;//weather day is holiday or not 
	private int weekday;//( 0 ~ 6)
	private int workingday;//if day is neither weekend nor holiday is 1, otherwise is 0.
	/**
	 * 1: Clear, Few clouds, Partly cloudy, Partly cloudy
	 * 2: Mist + Cloudy, Mist + Broken clouds, Mist + Few clouds, Mist
	 * 3: Light Snow, Light Rain + Thunderstorm + Scattered clouds, Light Rain + Scattered clouds
	 * 4: Heavy Rain + Ice Pallets + Thunderstorm + Mist, Snow + Fog
	 */
	private int weathersit;
	private double temp;// Normalized temperature in Celsius. The values are derived via (t-t_min)/(t_max-t_min), t_min=-8, t_max=+39 (only in hourly scale)
	private double atemp;//Normalized feeling temperature in Celsius. The values are derived via (t-t_min)/(t_max-t_min), t_min=-16, t_max=+50 (only in hourly scale)
	private double hum;//Normalized humidity. The values are divided to 100 (max)
	private double windspeed;//Normalized wind speed. The values are divided to 67 (max)
	private int casual;//count of casual users
	private int registered;//count of registered users
	private int cnt;//count of total rental bikes including both casual and registered
	public int getInstant() {
		return instant;
	}
	public void setInstant(int instant) {
		this.instant = instant;
	}
	public Date getDteday() {
		return dteday;
	}
	public void setDteday(Date dteday) {
		this.dteday = dteday;
	}
	public int getSeason() {
		return season;
	}
	public void setSeason(int season) {
		this.season = season;
	}
	public int getYr() {
		return yr;
	}
	public void setYr(int yr) {
		this.yr = yr;
	}
	public int getMnth() {
		return mnth;
	}
	public void setMnth(int mnth) {
		this.mnth = mnth;
	}
	public int getHr() {
		return hr;
	}
	public void setHr(int hr) {
		this.hr = hr;
	}
	public int getHoliday() {
		return holiday;
	}
	public void setHoliday(int holiday) {
		this.holiday = holiday;
	}
	public int getWeekday() {
		return weekday;
	}
	public void setWeekday(int weekday) {
		this.weekday = weekday;
	}
	public int getWorkingday() {
		return workingday;
	}
	public void setWorkingday(int workingday) {
		this.workingday = workingday;
	}
	public int getWeathersit() {
		return weathersit;
	}
	public void setWeathersit(int weathersit) {
		this.weathersit = weathersit;
	}
	public double getTemp() {
		return temp;
	}
	public void setTemp(double temp) {
		this.temp = temp;
	}
	public double getAtemp() {
		return atemp;
	}
	public void setAtemp(double atemp) {
		this.atemp = atemp;
	}
	public double getHum() {
		return hum;
	}
	public void setHum(double hum) {
		this.hum = hum;
	}
	public double getWindspeed() {
		return windspeed;
	}
	public void setWindspeed(double windspeed) {
		this.windspeed = windspeed;
	}
	public int getCasual() {
		return casual;
	}
	public void setCasual(int casual) {
		this.casual = casual;
	}
	public int getRegistered() {
		return registered;
	}
	public void setRegistered(int registered) {
		this.registered = registered;
	}
	public int getCnt() {
		return cnt;
	}
	public void setCnt(int cnt) {
		this.cnt = cnt;
	}
}
