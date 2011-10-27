package dispatch.s3

import java.net.URLEncoder._
import java.lang.Long
import dispatch._

object S3 {
  import javax.crypto

  import java.util.{Date,Locale,SimpleTimeZone}
  import java.text.SimpleDateFormat

  val UTF_8 = "UTF-8"

  val Root = "s3.amazonaws.com"

  object rfc822DateParser extends SimpleDateFormat("EEE, dd MMM yyyy HH:mm:ss z", Locale.US) {
    this.setTimeZone(new SimpleTimeZone(0, "GMT"))
  }

  def trim(s: String): String = s.dropWhile(_ == ' ').reverse.dropWhile(_ == ' ').reverse.toString

  def md5(bytes: Array[Byte]) = {
    import java.security.MessageDigest

    val r = MessageDigest.getInstance("MD5")
    r.reset
    r.update(bytes)
    new String(Request.encode_base64(r.digest))
  }

  def sign(method: String, path: String, secretKey: String, date: Date,
           contentType: Option[String], contentMd5: Option[String], amzHeaders: Map[String,Set[String]]): String = {
    sign(method, path, secretKey, Left(date), contentType, contentMd5, amzHeaders)
  }

  def sign(method: String, path: String, secretKey: String, dateOrExpires: Either[Date, Long],
           contentType: Option[String], contentMd5: Option[String], amzHeaders: Map[String,Set[String]]) = {
    val SHA1 = "HmacSHA1"
    val message = canonicalString(method, path, dateOrExpires, contentType, contentMd5, amzHeaders)
    val sig = {
      val mac = crypto.Mac.getInstance(SHA1)
      val key = new crypto.spec.SecretKeySpec(bytes(secretKey), SHA1)
      mac.init(key)
      new String(Request.encode_base64(mac.doFinal(bytes(message))))
    }
    sig
  }

  def signedUri(accessKey: String, secretKey: String, method:String, path: String, amzHeaders: Map[String, Set[String]],
                expires: Long = defaultExpiryTime,
                contentType: Option[String] = None, contentMd5: Option[String] = None) = {
    val signed = encode(sign(method, path, secretKey, Right(expires), contentType, contentMd5, amzHeaders), "UTF-8")
    "%s?Signature=%s&Expires=%s&AWSAccessKeyId=%s".format(path, signed, expires, accessKey)
  }

  def defaultExpiryTime = System.currentTimeMillis() / 1000 + 600

  /**
   * @return the canonical request string that needs to be signed for authentication
   */
  def canonicalString(method: String, path: String, dateOrExpires: Either[Date, Long], contentType: Option[String],
                      contentMd5: Option[String], amzHeaders: Map[String, Set[String]]) = {
    val amzString = amzHeaders.toList.sortWith(_._1.toLowerCase < _._1.toLowerCase).map{ case (k,v) => "%s:%s".format(k.toLowerCase, v.map(trim _).mkString(",")) }
    val dateExpiresString = dateOrExpires match {
      case Left(date) => rfc822DateParser.format(date)
      case Right(expires) => expires.toString
    }
    (method :: contentMd5.getOrElse("") :: contentType.getOrElse("") :: dateExpiresString :: Nil) ++ amzString ++ List(path) mkString("\n")
  }

  def bytes(s: String) = s.getBytes(UTF_8)

  implicit def Request2S3RequestSigner(r: Request) = new S3RequestSigner(r)
  implicit def Request2S3RequestSigner(r: String) = new S3RequestSigner(new Request(r))

  class S3RequestSigner(r: Request) {
    import org.apache.http.util.EntityUtils
    import org.apache.http.entity.BufferedHttpEntity

    type EntityHolder <: org.apache.http.message.BasicHttpEntityEnclosingRequest

    def <@ (accessKey: String, secretKey: String) = {
      val req = r.body.map { ent =>
        r <:< Map("Content-MD5" -> md5(EntityUtils.toByteArray(new BufferedHttpEntity(ent))))
      }.getOrElse(r)

      val path = req.to_uri.getPath

      val contentType = req.headers.filter {
        case (name, _) => name.toLowerCase == "content-type"
      }.headOption.map { case (_, value) => value }.orElse {
        req.body.map { _.getContentType.getValue }
      }
      val contentMd5 = req.headers.filter {
        case (name, _) => name.toLowerCase == "content-md5"
      }.headOption.map { case (_, value) => value }
      val d = new Date
      req <:< Map("Authorization" -> "AWS %s:%s".format(accessKey, sign(req.method, path, secretKey, d, contentType, contentMd5, amazonHeaders)),
        "Date" -> S3.rfc822DateParser.format(d))
    }

    def signed(accessKey: String, secretKey: String, expires: Long = defaultExpiryTime,
               contentType: Option[String] = None, contentMd5: Option[String] = None): Request = {
      val uri = signedUri(accessKey, secretKey, r.method, r.to_uri.getPath, amazonHeaders, expires, contentType, contentMd5)
      val requestHeaders = for {
        (key, Some(value)) <- Map("Content-Type" -> contentType, "Content-Md5" -> contentMd5)
      } yield (key -> value)
      (:/(S3.Root) / uri.substring(1)).copy(method = r.method, headers = r.headers).secure <:< requestHeaders
    }

    private def amazonHeaders = r.headers.filter {
        case (name, _) => name.toLowerCase.startsWith("x-amz")
      }.foldLeft(Map.empty[String, Set[String]]) { case (m, (name, value)) =>
        m + (name -> (m.getOrElse(name, Set.empty[String]) + value))
      }
  }
}

class Bucket(val name: String) extends Request(:/(S3.Root) / name) {
  val create = this <<< ""
}
object Bucket {
  def apply(name: String) = new Bucket(name)
}
