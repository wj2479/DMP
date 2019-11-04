package com.wj.dmp.parquet

import com.wj.dmp.utils.NumUtils

class Log(val sessionid: String, //会话标识
          val advertisers: Int, //广告主id
          val adorderid: Int, //广告id
          val adcreativeid: Int, //广告创意id   ( >= 200000 : dsp ,  < 200000 oss)
          val adplatformproviderid: Int, //广告平台商id  (>= 100000: rtb  , < 100000 : api )
          val sdkversion: String, //sdk版本号
          val adplatformkey: String, //平台商key
          val putinmodeltype: Int, //针对广告主的投放模式,1：展示量投放 2：点击量投放
          val requestmode: Int, //数据请求方式（1:请求、2:展示、3:点击）
          val adprice: Double, //广告价格
          val adppprice: Double, //平台商价格
          val requestdate: String, //请求时间,格式为：yyyy-m-dd hh:mm:ss
          val ip: String, //设备用户的真实ip地址
          val appid: String, //应用id
          val appname: String, //应用名称
          val uuid: String, //设备唯一标识，比如imei或者androidid等
          val device: String, //设备型号，如htc、iphone
          val client: Int, //设备类型 （1：android 2：ios 3：wp）
          val osversion: String, //设备操作系统版本，如4.0
          val density: String, //备屏幕的密度 android的取值为0.75、1、1.5,ios的取值为：1、2
          val pw: Int, //设备屏幕宽度
          val ph: Int, //设备屏幕高度
          val lng: String, //设备所在经度
          val lat: String, //设备所在纬度
          val provincename: String, //设备所在省份名称
          val cityname: String, //设备所在城市名称
          val ispid: Int, //运营商id
          val ispname: String, //运营商名称
          val networkmannerid: Int, //联网方式id
          val networkmannername: String, //联网方式名称
          val iseffective: Int, //有效标识（有效指可以正常计费的）(0：无效 1：有效)
          val isbilling: Int, //是否收费（0：未收费 1：已收费）
          val adspacetype: Int, //广告位类型（1：banner 2：插屏 3：全屏）
          val adspacetypename: String, //广告位类型名称（banner、插屏、全屏）
          val devicetype: Int, //设备类型（1：手机 2：平板）
          val processnode: Int, //流程节点（1：请求量kpi 2：有效请求 3：广告请求）
          val apptype: Int, //应用类型id
          val district: String, //设备所在县名称
          val paymode: Int, //针对平台商的支付模式，1：展示量投放(CPM) 2：点击量投放(CPC)
          val isbid: Int, //是否rtb
          val bidprice: Double, //rtb竞价价格
          val winprice: Double, //rtb竞价成功价格
          val iswin: Int, //是否竞价成功
          val cur: String, //values:usd|rmb等
          val rate: Double, //汇率
          val cnywinprice: Double, //rtb竞价成功转换成人民币的价格
          val imei: String, //imei
          val mac: String, //mac
          val idfa: String, //idfa
          val openudid: String, //openudid
          val androidid: String, //androidid
          val rtbprovince: String, //rtb 省
          val rtbcity: String, //rtb 市
          val rtbdistrict: String, //rtb 区
          val rtbstreet: String, //rtb 街道
          val storeurl: String, //app的市场下载地址
          val realip: String, //真实ip
          val isqualityapp: Int, //优选标识
          val bidfloor: Double, //底价
          val aw: Int, //广告位的宽
          val ah: Int, //广告位的高
          val imeimd5: String, //imei_md5
          val macmd5: String, //mac_md5
          val idfamd5: String, //idfa_md5
          val openudidmd5: String, //openudid_md5
          val androididmd5: String, //androidid_md5
          val imeisha1: String, //imei_sha1
          val macsha1: String, //mac_sha1
          val idfasha1: String, //idfa_sha1
          val openudidsha1: String, //openudid_sha1
          val androididsha1: String, //androidid_sha1
          val uuidunknow: String, //uuid_unknow tanx密文
          val userid: String, //平台用户id
          val iptype: Int, //表示ip库类型，1为点媒ip库，2为广告协会的ip地理信息标准库，默认为1
          val initbidprice: Double, //初始出价
          val adpayment: Double, //转换后的广告消费（保留小数点后6位）
          val agentrate: Double, //代理商利润率
          val lomarkrate: Double, //代理利润率
          val adxrate: Double, //媒介利润率
          val title: String, //标题
          val keywords: String, //关键字
          val tagid: String, //广告位标识(当视频流量时值为视频ID号)
          val callbackdate: String, //回调时间 格式为:YYYY/mm/dd hh:mm:ss
          val channelid: String, //频道ID
          val mediatype: Int
         ) extends Product with Serializable {

  override def productElement(n: Int): Any = n match {
    case 0 => sessionid
    case 1 => advertisers
    case 2 => adorderid
    case 3 => adcreativeid
    case 4 => adplatformproviderid
    case 5 => sdkversion
    case 6 => adplatformkey
    case 7 => putinmodeltype
    case 8 => requestmode
    case 9 => adprice
    case 10 => adppprice
    case 11 => requestdate
    case 12 => ip
    case 13 => appid
    case 14 => appname
    case 15 => uuid
    case 16 => device
    case 17 => client
    case 18 => osversion
    case 19 => density
    case 20 => pw
    case 21 => ph
    case 22 => lng
    case 23 => lat
    case 24 => provincename
    case 25 => cityname
    case 26 => ispid
    case 27 => ispname
    case 28 => networkmannerid
    case 29 => networkmannername
    case 30 => iseffective
    case 31 => isbilling
    case 32 => adspacetype
    case 33 => adspacetypename
    case 34 => devicetype
    case 35 => processnode
    case 36 => apptype
    case 37 => district
    case 38 => paymode
    case 39 => isbid
    case 40 => bidprice
    case 41 => winprice
    case 42 => iswin
    case 43 => cur
    case 44 => rate
    case 45 => cnywinprice
    case 46 => imei
    case 47 => mac
    case 48 => idfa
    case 49 => openudid
    case 50 => androidid
    case 51 => rtbprovince
    case 52 => rtbcity
    case 53 => rtbdistrict
    case 54 => rtbstreet
    case 55 => storeurl
    case 56 => realip
    case 57 => isqualityapp
    case 58 => bidfloor
    case 59 => aw
    case 60 => ah
    case 61 => imeimd5
    case 62 => macmd5
    case 63 => idfamd5
    case 64 => openudidmd5
    case 65 => androididmd5
    case 66 => imeisha1
    case 67 => macsha1
    case 68 => idfasha1
    case 69 => openudidsha1
    case 70 => androididsha1
    case 71 => uuidunknow
    case 72 => userid
    case 73 => iptype
    case 74 => initbidprice
    case 75 => adpayment
    case 76 => agentrate
    case 77 => lomarkrate
    case 78 => adxrate
    case 79 => title
    case 80 => keywords
    case 81 => tagid
    case 82 => callbackdate
    case 83 => channelid
    case 84 => mediatype
  }

  override def productArity: Int = 85

  override def canEqual(that: Any): Boolean = that.equals(this)
}


object Log {
  def apply(arr: Array[String]): Log = new Log(arr(0),
    NumUtils.toInt(arr(1)),
    NumUtils.toInt(arr(2)),
    NumUtils.toInt(arr(3)),
    NumUtils.toInt(arr(4)),
    arr(5),
    arr(6),
    NumUtils.toInt(arr(7)),
    NumUtils.toInt(arr(8)),
    NumUtils.toDouble(arr(9)),
    NumUtils.toDouble(arr(10)),
    arr(11),
    arr(12),
    arr(13),
    arr(14),
    arr(15),
    arr(16),
    NumUtils.toInt(arr(17)),
    arr(18),
    arr(19),
    NumUtils.toInt(arr(20)),
    NumUtils.toInt(arr(21)),
    arr(22),
    arr(23),
    arr(24),
    arr(25),
    NumUtils.toInt(arr(26)),
    arr(27),
    NumUtils.toInt(arr(28)),
    arr(29),
    NumUtils.toInt(arr(30)),
    NumUtils.toInt(arr(31)),
    NumUtils.toInt(arr(32)),
    arr(33),
    NumUtils.toInt(arr(34)),
    NumUtils.toInt(arr(35)),
    NumUtils.toInt(arr(36)),
    arr(37),
    NumUtils.toInt(arr(38)),
    NumUtils.toInt(arr(39)),
    NumUtils.toDouble(arr(40)),
    NumUtils.toDouble(arr(41)),
    NumUtils.toInt(arr(42)),
    arr(43),
    NumUtils.toDouble(arr(44)),
    NumUtils.toDouble(arr(45)),
    arr(46),
    arr(47),
    arr(48),
    arr(49),
    arr(50),
    arr(51),
    arr(52),
    arr(53),
    arr(54),
    arr(55),
    arr(56),
    NumUtils.toInt(arr(57)),
    NumUtils.toDouble(arr(58)),
    NumUtils.toInt(arr(59)),
    NumUtils.toInt(arr(60)),
    arr(61),
    arr(62),
    arr(63),
    arr(64),
    arr(65),
    arr(66),
    arr(67),
    arr(68),
    arr(69),
    arr(70),
    arr(71),
    arr(72),
    NumUtils.toInt(arr(73)),
    NumUtils.toDouble(arr(74)),
    NumUtils.toDouble(arr(75)),
    NumUtils.toDouble(arr(76)),
    NumUtils.toDouble(arr(77)),
    NumUtils.toDouble(arr(78)),
    arr(79),
    arr(80),
    arr(81),
    arr(82),
    arr(83),
    NumUtils.toInt(arr(84)))
}