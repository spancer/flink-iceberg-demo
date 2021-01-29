package com.coomia.datalake.kafka;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.UUID;
import java.util.stream.Collectors;

/**
 * Random some user evevnt data for tests only.
 * 
 * @author spancer
 * @date 2019/03/19
 */
public class RandomEventDataUtil {

  /**
   * 随机一个IP
   * 
   * @return
   */
  public static String randomIP() {
    Random r = new Random();
    return r.nextInt(256) + "." + r.nextInt(256) + "." + r.nextInt(256) + "." + r.nextInt(256);
  }

  /**
   * 随机一个平台
   * 
   * @return
   */
  public static String randomPlatform() {
    List<String> list =
        Arrays.asList("Android", "iOS", "C#", "Java", "Python", "JS", "Wechat", "PHP", "Go");
    return list.get((int) (Math.random() * list.size()));
  }

  public static String randomOS() {
    List<String> list = Arrays.asList("Android", "iOS", "Windows");
    return list.get((int) (Math.random() * list.size()));
  }

  /**
   * 随机一个PV
   * 
   * @return
   */
  public static String randomEvent() {
    List<String> list = Arrays.asList("ViewProduct", "ViewComment", "AddShoppingCart", "Pay",
        "CancelOrder", "Comment", "Search Product", "ViewPicture", "JumpOut", "epage_click",
        "bindWechat", "Start", "End");
    return list.get((int) (Math.random() * list.size()));
  }

  public static String randomValue(String... values) {
    List<String> list = Arrays.asList(values);
    return list.get((int) (Math.random() * list.size()));
  }

  public static Integer randomValue(Integer... values) {
    List<Integer> list = Arrays.asList(values);
    return list.get((int) (Math.random() * list.size()));
  }

  /**
   * 
   * @param beginDate
   * @param endDate
   * @return
   */
  public static long randomDate(String beginDate, String endDate) {
    Date end = null;
    Date start = null;
    SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd");
    try {
      start = format.parse(beginDate);
      if (null == endDate)
        end = new Date();
      else
        end = format.parse(endDate);
      if (start.getTime() >= end.getTime()) {
        return end.getTime();
      }
      long date = random(start.getTime(), end.getTime());
      return date;
    } catch (Exception e) {
      e.printStackTrace();
    }
    return new Date().getTime();
  }

  private static long random(long begin, long end) {
    long rtn = begin + (long) (Math.random() * (end - begin));
    if (rtn == begin || rtn == end) {
      return random(begin, end);
    }
    return rtn;
  }

  public static int getNum(int start, int end) {
    return (int) (Math.random() * (end - start + 1) + start);
  }

  private static String[] telFirst =
      "134,135,136,137,138,139,150,151,152,157,158,159,130,131,132,155,156,133,153".split(",");

  private static String randomTel() {
    int index = getNum(0, telFirst.length - 1);
    String first = telFirst[index];
    String second = String.valueOf(getNum(1, 888) + 10000).substring(1);
    String third = String.valueOf(getNum(1, 9100) + 10000).substring(1);
    return first + second + third;
  }

  /**
   * 随机属性
   * 
   * @return
   */
  public static Map<String, Object> randomEventProperties(String event) {
    Map<String, Object> trackPropertie = new HashMap<String, Object>();
    trackPropertie.put("$ip", randomIP()); // IP地址
    trackPropertie.put("$is_login", new Random().nextInt(3) == 1);
    trackPropertie.put("$lib", randomPlatform());
    String os = randomOS();
    trackPropertie.put("$os", os);
    trackPropertie.put("$os_version", new Random().nextInt(10));
    trackPropertie.put("$referrer", "http://www." + UUID.randomUUID().toString() + ".com");
    trackPropertie.put("$screen_height", randomValue(720, 600, 640));
    trackPropertie.put("$screen_width", randomValue(1280, 800, 960, 1136, 1334));
    trackPropertie.put("$brand", "品牌" + new Random().nextInt(20));
    trackPropertie.put("$network", randomValue("WIFI", "2G", "3G", "4G", "5G"));
    trackPropertie.put("$carrier_name", randomValue("中国联通", "中国移动", "中国电信"));
    trackPropertie.put("$utm_medium", "utm-medium-" + new Random().nextInt(200));
    trackPropertie.put("$utm_source", "utm_source-" + new Random().nextInt(200));
    trackPropertie.put("$utm_campaign", "utm_campaign-" + new Random().nextInt(200));
    trackPropertie.put("openID", "wechatID-" + UUID.randomUUID().toString());

    if (event.equals("epage_click")) {
      int pageID = new Random().nextInt(2000);
      String type = randomValue("优惠券", "营销广告", "商品促销");
      trackPropertie.put("pageID", "pageID-" + pageID);
      trackPropertie.put("pageName", "pageName-" + pageID);
      trackPropertie.put("contentType", type);
      trackPropertie.put("propID", type + "-" + new Random().nextInt(2200));
      trackPropertie.put("propID", "群嗨-" + new Random().nextInt(500));

    } else if (event.equals("Pay")) {
      trackPropertie.put("paymentMethod", randomValue("AliPay", "微信支付"));
    } else if (event.equals("bindWechat")) {
      trackPropertie.put("phoneNum", randomTel());
    }
    String productType = randomValue("书籍", "上衣 ", "手机", "女鞋", "男鞋", "男包", "电子产品", "电脑");
    List<String> bookList = new ArrayList<String>();
    bookList.add(productType + "名-" + new Random().nextInt(200));
    trackPropertie.put("productName", bookList); // 商品列表
    trackPropertie.put("productType", productType);// 商品类别
    trackPropertie.put("producePrice", new Random().nextInt()); // 商品价格
    trackPropertie.put("shop", randomValue("百货商店", "王府井", "步步高", "超市", "7-11", "悦方", "便利店"));
    return trackPropertie;
  }

  /**
   * 随机一条记录
   * 
   * @return
   */
  public static Map<String, Object> randomRecord(int dau) {
    Map<String, Object> result = new HashMap<String, Object>();
    String xwhat = randomEvent();
    result.put("uid", "User-" + new Random().nextInt(dau));
    result.put("eventid", xwhat);
    result.put("eventTime", randomDate("2021-01-01", null));
    result.put("uuid", UUID.randomUUID().toString());
    result = result.entrySet().stream().sorted(Map.Entry.comparingByKey())
        .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue,
            (oldValue, newValue) -> oldValue, LinkedHashMap::new));
    return result;
  }

  public static void main(String[] args) {
    System.out.println(randomRecord(10));
  }

}
