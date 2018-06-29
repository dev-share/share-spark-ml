package com.glocalme.share.spark.vsim.pojo;

import java.io.Serializable;
import java.util.Date;

import com.glocalme.share.spark.util.DateUtil;

public class TerminalFlowUpload implements Serializable {
	private static final long serialVersionUID = 8555236853037003802L;
	private String _id;
	private String cellId;
	private long cid;
	private int devicetype;
	private String iso2;
	private String lac;
	private String latitude;
	private String longitude;
	private String logId;
	private String mcc;
	private String plmn;
	private String uid;
	private String country;
	private String province;
	private String city;
	private String sessionid;
	private String userCode;
	private long imei;
	private String imsi;
	private long beginTime;
	private long countDay;
	private int card;
	private long cardDownFlow;
	private long cardUpFlow;
	private long sys;
	private long sysDownFlow;
	private long sysUpFlow;
	private long userDownFlow;
	private long userUpFlow;
	private long user;
	private long user1;
	private long user2;
	/**
	 * 休眠标记(0.不休眠,1.休眠)
	 */
	private int sleep = -1;
	
	public String get_id() {
		return _id;
	}
	public void set_id(String _id) {
		this._id = _id;
	}
	public String getCellId() {
		return cellId;
	}
	public void setCellId(String cellId) {
		this.cellId = cellId;
	}
	public long getCid() {
		return cid;
	}
	public void setCid(long cid) {
		this.cid = cid;
	}
	public int getDevicetype() {
		return devicetype;
	}
	public void setDevicetype(int devicetype) {
		this.devicetype = devicetype;
	}
	public String getIso2() {
		return iso2;
	}
	public void setIso2(String iso2) {
		this.iso2 = iso2;
	}
	public String getLac() {
		return lac;
	}
	public void setLac(String lac) {
		this.lac = lac;
	}
	public String getLatitude() {
		return latitude;
	}
	public void setLatitude(String latitude) {
		this.latitude = latitude;
	}
	public String getLongitude() {
		return longitude;
	}
	public void setLongitude(String longitude) {
		this.longitude = longitude;
	}
	public String getLogId() {
		return logId;
	}
	public void setLogId(String logId) {
		this.logId = logId;
	}
	public String getMcc() {
		return mcc;
	}
	public void setMcc(String mcc) {
		this.mcc = mcc;
	}
	public String getPlmn() {
		return plmn;
	}
	public void setPlmn(String plmn) {
		this.plmn = plmn;
	}
	public String getUid() {
		return uid;
	}
	public void setUid(String uid) {
		this.uid = uid;
	}
	public String getCountry() {
		return country;
	}
	public void setCountry(String country) {
		this.country = country;
	}
	public String getProvince() {
		return province;
	}
	public void setProvince(String province) {
		this.province = province;
	}
	public String getCity() {
		return city;
	}
	public void setCity(String city) {
		this.city = city;
	}
	public String getSessionid() {
		return sessionid;
	}
	public void setSessionid(String sessionid) {
		this.sessionid = sessionid;
	}
	public String getUserCode() {
		return userCode;
	}
	public void setUserCode(String userCode) {
		this.userCode = userCode;
	}
	public long getImei() {
		return imei;
	}
	public void setImei(long imei) {
		this.imei = imei;
	}
	public String getImsi() {
		return imsi;
	}
	public void setImsi(String imsi) {
		this.imsi = imsi;
	}
	public long getBeginTime() {
		return beginTime;
	}
	public void setBeginTime(long beginTime) {
		this.beginTime = beginTime;
	}
	public long getCountDay() {
		return countDay;
	}
	public void setCountDay(long countDay) {
		this.countDay = countDay;
	}
	public int getCard() {
		return card;
	}
	public void setCard(int card) {
		this.card = card;
	}
	public long getCardDownFlow() {
		return cardDownFlow;
	}
	public void setCardDownFlow(long cardDownFlow) {
		this.cardDownFlow = cardDownFlow;
	}
	public long getCardUpFlow() {
		return cardUpFlow;
	}
	public void setCardUpFlow(long cardUpFlow) {
		this.cardUpFlow = cardUpFlow;
	}
	public long getSys() {
		return sys;
	}
	public void setSys(long sys) {
		this.sys = sys;
	}
	public long getSysDownFlow() {
		return sysDownFlow;
	}
	public void setSysDownFlow(long sysDownFlow) {
		this.sysDownFlow = sysDownFlow;
	}
	public long getSysUpFlow() {
		return sysUpFlow;
	}
	public void setSysUpFlow(long sysUpFlow) {
		this.sysUpFlow = sysUpFlow;
	}
	public long getUserDownFlow() {
		return userDownFlow;
	}
	public void setUserDownFlow(long userDownFlow) {
		this.userDownFlow = userDownFlow;
	}
	public long getUserUpFlow() {
		return userUpFlow;
	}
	public void setUserUpFlow(long userUpFlow) {
		this.userUpFlow = userUpFlow;
	}
	public long getUser() {
		return user;
	}
	public void setUser(long user) {
		this.user = user;
	}
	public long getUser1() {
		return user1;
	}
	public void setUser1(long user1) {
		this.user1 = user1;
	}
	public long getUser2() {
		return user2;
	}
	public void setUser2(long user2) {
		this.user2 = user2;
	}
	public int getSleep() {
		return sleep;
	}
	public void setSleep(int sleep) {
		this.sleep = sleep;
	}
	public String getBeginTimeStr(){
		String dateTime = null;
		if(this.getBeginTime()>0){
			dateTime = DateUtil.formatDateHMTimeStr(new Date(this.getBeginTime()));
		}
		return dateTime;
	}
	public String getCountDayStr(){
		String dateTime = null;
		if(this.getCountDay()>0){
			dateTime = DateUtil.formatDateHMTimeStr(new Date(this.getCountDay()));
		}
		return dateTime;
	}
}
