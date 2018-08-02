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

}
