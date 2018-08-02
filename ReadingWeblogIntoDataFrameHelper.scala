import java.net.URLDecoder
import scala.collection.mutable.Map

object ReadingWeblogIntoDataFrameHelper {

  case class webrow(cip:String,dash:String,user:String
                    ,datetime:String,tagcall:String,status:Int
                    ,bytes:Int  ,referrer:String,ua:String
                    ,cookie:String,xff:String)

  val withNames = new util.matching.Regex("^(.*)\\s(-)\\s(.*)\\s\\[(.*)\\]\\s\"GET\\s(.*?)\"\\s(.*?)\\s(.*?)\\s\"(.*?)\"\\s\"(.*?)\"\\s\"CP_GUTC=(.*?)\"\\s\"(.*?)\"","cip","dash", "user","datetime","tagcall","status","bytes","referrer","ua","cookie","xff")
  val replaceurlStart : scala.util.matching.Regex = "http(|s):".r


  def weblogparser(inline:String) : Option[webrow] = {

    val Some(inlinearray) = withNames findFirstMatchIn inline
    val referrer = replaceurlStart.replaceFirstIn(inlinearray.group("referrer"),"")

    return Some(webrow(inlinearray.group("cip")
      ,inlinearray.group("dash")
      ,inlinearray.group("user")
      ,inlinearray.group("datetime")
      ,inlinearray.group("tagcall")
      ,inlinearray.group("status").toInt
      ,inlinearray.group("bytes").toInt
      ,referrer
      //,inlinearray.group("referrer")
      ,inlinearray.group("ua")
      ,inlinearray.group("cookie")
      ,inlinearray.group("xff")))
  }


  def main(args: Array[String]) {
    val str = "js=1&ts=1530399258370.919&lc=https%3A%2F%2Fapps.cisco.com%2FCommerce%2Fdeal&rf=https%3A%2F%2Fapps.cisco.com%2FCommerce%2Fhome&rs=1024x768&cd=24&ln=en&tz=GMT%20%2B01%3A00&jv=0&ck=CP_GUTC%3D173.37.216.11.1530399232728034&lpos=nextgenworkspace&lid=deal&linktext=deal_export_delicondown&link=javascript%3Avoid(0)%3B&ev=link&tag=ut4.39.201806211831&utag_main_v_id=016452e7108a0013fad2b0ab44c30406c002f06400718&vid=173.37.216.11.1530399232728034&title=cisco%20commerce%20deals%20%26%20quotes&url=https%3A%2F%2Fapps.cisco.com%2Fcommerce%2Fdeal%23!%26menu%3Dadvancesearch&entitlement=3&locale=en-us&meta.country=us&meta.locale=us&breakpoint=unavailable&content_type=no%20contenttype&linktrack=linkpage&loc=http%3A%2F%2Fapps.cisco.com%2Fcommerce%2Fdeal&cookie_length=2000&meta.iapath=no%20iapath&hier1=no%20iapath&meta.wm_reporting_category=no%20iapath&sa_source=meta.iapath&t_profile=cisco.commerce&t_load=ctm&suite=cisco-complete&cookies=true&localstorage=true&dnt=false&atg=1&jumplink=false&linkhierarchy=nextgenworkspace%3Adeal%3Adeal_export_delicondown&adobeVersions=AppMeasurement%3D2.6.0%2CVisitorJS%3Dna%2CMbox%3Dna&ets=1530399599421.138"
    val myhashmap: Map[String, String] = Map.empty[String, String]
    val listing: Array[String] = (str.split("&")) //.flatMap(_.split("(.*)=(.*)"))
    //listing.foreach(println)
    for (item <- listing) {
      val KeyValue: Array[String] = item.split("=")
      myhashmap += (KeyValue(0) -> URLDecoder.decode(KeyValue(1),"UTF-8"))
    }
    println(s"Elements of hashMap1 = $myhashmap")

  }
}
