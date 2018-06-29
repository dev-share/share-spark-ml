package com.ucloudlink.spark.pojo.classify;

import java.io.Serializable;

public class Train implements Serializable {
	private static final long serialVersionUID = 2216867159889181029L;
	private String url;//
	private int urlid;//页面的ID、
	private Boilerplate boilerplate;//原始的文本内容
	private String alchemy_category;//分配给页面的类别
	private double alchemy_category_score;//double ?=0.0
	private double avglinksize;
	private double commonlinkratio_1;
	private double commonlinkratio_2;
	private double commonlinkratio_3;
	private double commonlinkratio_4;
	private double compression_ratio;
	private double embed_ratio;
	private int framebased;
	private double frameTagRatio;
	private int hasDomainLink;
	private double html_ratio;
	private double image_ratio;
	private int is_news;//int ?=0
	private int lengthyLinkDomain;
	private double linkwordscore;
	private int news_front_page;//int ?=1
	private int non_markup_alphanum_characters;
	private int numberOfLinks;
	private double numwords_in_url;
	private double parametrizedLinkRatio;
	private double spelling_errors_ratio;
	private int label;	//目标值:1为长久，0为短暂
	public String getUrl() {
		return url;
	}
	public void setUrl(String url) {
		this.url = url;
	}
	public int getUrlid() {
		return urlid;
	}
	public void setUrlid(int urlid) {
		this.urlid = urlid;
	}
	public Boilerplate getBoilerplate() {
		return boilerplate;
	}
	public void setBoilerplate(Boilerplate boilerplate) {
		this.boilerplate = boilerplate;
	}
	public String getAlchemy_category() {
		return alchemy_category;
	}
	public void setAlchemy_category(String alchemy_category) {
		this.alchemy_category = alchemy_category;
	}
	public double getAlchemy_category_score() {
		return alchemy_category_score;
	}
	public void setAlchemy_category_score(double alchemy_category_score) {
		this.alchemy_category_score = alchemy_category_score;
	}
	public double getAvglinksize() {
		return avglinksize;
	}
	public void setAvglinksize(double avglinksize) {
		this.avglinksize = avglinksize;
	}
	public double getCommonlinkratio_1() {
		return commonlinkratio_1;
	}
	public void setCommonlinkratio_1(double commonlinkratio_1) {
		this.commonlinkratio_1 = commonlinkratio_1;
	}
	public double getCommonlinkratio_2() {
		return commonlinkratio_2;
	}
	public void setCommonlinkratio_2(double commonlinkratio_2) {
		this.commonlinkratio_2 = commonlinkratio_2;
	}
	public double getCommonlinkratio_3() {
		return commonlinkratio_3;
	}
	public void setCommonlinkratio_3(double commonlinkratio_3) {
		this.commonlinkratio_3 = commonlinkratio_3;
	}
	public double getCommonlinkratio_4() {
		return commonlinkratio_4;
	}
	public void setCommonlinkratio_4(double commonlinkratio_4) {
		this.commonlinkratio_4 = commonlinkratio_4;
	}
	public double getCompression_ratio() {
		return compression_ratio;
	}
	public void setCompression_ratio(double compression_ratio) {
		this.compression_ratio = compression_ratio;
	}
	public double getEmbed_ratio() {
		return embed_ratio;
	}
	public void setEmbed_ratio(double embed_ratio) {
		this.embed_ratio = embed_ratio;
	}
	public int getFramebased() {
		return framebased;
	}
	public void setFramebased(int framebased) {
		this.framebased = framebased;
	}
	public double getFrameTagRatio() {
		return frameTagRatio;
	}
	public void setFrameTagRatio(double frameTagRatio) {
		this.frameTagRatio = frameTagRatio;
	}
	public int getHasDomainLink() {
		return hasDomainLink;
	}
	public void setHasDomainLink(int hasDomainLink) {
		this.hasDomainLink = hasDomainLink;
	}
	public double getHtml_ratio() {
		return html_ratio;
	}
	public void setHtml_ratio(double html_ratio) {
		this.html_ratio = html_ratio;
	}
	public double getImage_ratio() {
		return image_ratio;
	}
	public void setImage_ratio(double image_ratio) {
		this.image_ratio = image_ratio;
	}
	public int getIs_news() {
		return is_news;
	}
	public void setIs_news(int is_news) {
		this.is_news = is_news;
	}
	public int getLengthyLinkDomain() {
		return lengthyLinkDomain;
	}
	public void setLengthyLinkDomain(int lengthyLinkDomain) {
		this.lengthyLinkDomain = lengthyLinkDomain;
	}
	public double getLinkwordscore() {
		return linkwordscore;
	}
	public void setLinkwordscore(double linkwordscore) {
		this.linkwordscore = linkwordscore;
	}
	public int getNews_front_page() {
		return news_front_page;
	}
	public void setNews_front_page(int news_front_page) {
		this.news_front_page = news_front_page;
	}
	public int getNon_markup_alphanum_characters() {
		return non_markup_alphanum_characters;
	}
	public void setNon_markup_alphanum_characters(int non_markup_alphanum_characters) {
		this.non_markup_alphanum_characters = non_markup_alphanum_characters;
	}
	public int getNumberOfLinks() {
		return numberOfLinks;
	}
	public void setNumberOfLinks(int numberOfLinks) {
		this.numberOfLinks = numberOfLinks;
	}
	public double getNumwords_in_url() {
		return numwords_in_url;
	}
	public void setNumwords_in_url(double numwords_in_url) {
		this.numwords_in_url = numwords_in_url;
	}
	public double getParametrizedLinkRatio() {
		return parametrizedLinkRatio;
	}
	public void setParametrizedLinkRatio(double parametrizedLinkRatio) {
		this.parametrizedLinkRatio = parametrizedLinkRatio;
	}
	public double getSpelling_errors_ratio() {
		return spelling_errors_ratio;
	}
	public void setSpelling_errors_ratio(double spelling_errors_ratio) {
		this.spelling_errors_ratio = spelling_errors_ratio;
	}
	public int getLabel() {
		return label;
	}
	public void setLabel(int label) {
		this.label = label;
	}
	
	public class Boilerplate implements Serializable {
		private static final long serialVersionUID = 2375089666334419468L;
		private String title;
		private String body;
		private String url;
		public String getTitle() {
			return title;
		}
		public void setTitle(String title) {
			this.title = title;
		}
		public String getBody() {
			return body;
		}
		public void setBody(String body) {
			this.body = body;
		}
		public String getUrl() {
			return url;
		}
		public void setUrl(String url) {
			this.url = url;
		}
	}
}
