import java.util.{TimeZone, Calendar}
import org.specs._
import dispatch._
import s3._
import S3._
import java.io.{File,FileWriter}
import scala.io.Source

object S3Spec extends Specification {

  val UTF_8 = "UTF-8"
  val bucketName = "databinder-dispatch-s3-test-bucket"
  val test = Bucket(bucketName)
  val access_key = getValue("awsAccessKey")
  val secret_key = getValue("awsSecretAccessKey")

  def shouldWeSkip_? = List(access_key, secret_key) must notContain(None).orSkip

  def newTempFile = {
    val testFile = File.createTempFile("s3specs","bin")
    val writer = new FileWriter(testFile)
    writer.write("testing")
    writer.close
    testFile
  }

  "S3" should {
    "be able to create a bucket" in {
      shouldWeSkip_?
      val resp = test.create <@ (access_key.get, secret_key.get)
      Http x (resp as_str) {
        case (code, _, _, _) => code must be_==(200)
      }
    }
    "be able to create a file" in {
      shouldWeSkip_?

      val testFile = newTempFile

      val r = test / "testing.txt" <<< (testFile, "plain/text") <@ (access_key.get, secret_key.get)
      Http x (r as_str){
        case (code, _, _, _) => {
          code must be_==(200)
        }
      }
    }
    "be able to send x-amz headers" in {
      shouldWeSkip_?
      val testFile = newTempFile
      val headers = Map("x-amz-meta-author" -> "john@doe.com")
      val r = test / "testing.txt" <<< (testFile, "plain/text") <:< headers <@ (access_key.get, secret_key.get)
      Http x (r as_str) {
        case (code, _, _, _) => code must be_== (200)
      }
    }
    "be able to retrieve a file" in {
      shouldWeSkip_?
      Http x(test / "testing.txt" <@ (access_key.get, secret_key.get) as_str) {
        case (code, _, _, str) => {
          code must be_==(200)
          str() must be_==("testing")
        }
      }
    }
    "be able to delete a file" in {
      shouldWeSkip_?
      Http x (test.DELETE / "testing.txt" <@ (access_key.get, secret_key.get) >|) {
        case (code, _, _, _) => code must be_==(204)
      }
    }
    "create the correct canonical string for a request with a date header" in {
      val expected = "PUT\nmd5sum\ntext/plain\nMon, 17 Oct 2011 11:43:29 GMT\nx-amz-meta-author:john@doe.com\n/mybucket/newobject123"
      val cal = Calendar.getInstance(TimeZone.getTimeZone("GMT"))
      cal.set(2011, 9, 17, 11, 43, 29)
      val date = cal.getTime()
      val amzHeaders = Map("x-amz-meta-author" -> Set("john@doe.com"))
      val res = canonicalString("PUT", "/mybucket/newobject123", Left(date), Some("text/plain"), Some("md5sum"), amzHeaders)
      res must_== expected
    }
    "create the correct canonical string for a request with an Expires value" in {
      val expires = System.currentTimeMillis() / 1000 + 30
      val expected = "PUT\n\n\n%s\nx-amz-meta-author:john@doe.com\n/mybucket/newobject123".format(expires.toString)
      val amzHeaders = Map("x-amz-meta-author" -> Set("john@doe.com"))
      val res = canonicalString("PUT", "/mybucket/newobject123", Right(expires), None, None, amzHeaders)
      res must_== expected
    }
    "create a correct signed URI that can be used by a third party for creating a file" in {
      shouldWeSkip_?
      val testFile = newTempFile
      val expires = System.currentTimeMillis() / 1000 + 30
      val contentMd5 = md5(Source.fromFile(testFile).map(_.toByte).toArray)
      val amzHeaders = Map("x-amz-meta-author" -> "john@doe.com")
      val req = ((test / "testfile123.txt") <:< amzHeaders).PUT.signed(
        access_key.get, secret_key.get, expires, Some("text/plain"), Some(contentMd5))
      req.to_uri.toString must startWith ("https://s3.amazonaws.com/databinder-dispatch-s3-test-bucket/testfile123.txt")
      Http x (req <<< (testFile, "text/plain")) >| {
        case (code, _, _, _) => {
          code must be_== (200)
        }
      }
    }
    "create a correct signed URI that can be used by a third party for retrieving a file" in {
      shouldWeSkip_?
      val expires = System.currentTimeMillis() / 1000 + 30
      val req = (test / "testfile123.txt").signed(access_key.get, secret_key.get, expires)
      req.to_uri.toString must startWith ("https://s3.amazonaws.com/databinder-dispatch-s3-test-bucket/testfile123.txt")
      Http x(req as_str) {
        case (code, _, _, str) => {
          code must be_==(200)
          str() must be_==("testing")
        }
      }
    }
      "create a correct signed URI that can be used by a third party for deleting a file" in {
      shouldWeSkip_?
      val expires = System.currentTimeMillis() / 1000 + 30
      val req = (test / "testfile123.txt").DELETE signed (access_key.get, secret_key.get, expires)
      Http x req >| {
        case (code, _, _, _) => {
          code must be_== (204)
        }
      }
    }
    "be able to delete a bucket" in {
      shouldWeSkip_?
      Http x (test.DELETE <@ (access_key.get, secret_key.get) >| ) {
        case (code, _, _, _) => code must be_==(204)
      }
    }

  }

  doAfterSpec {
    Http.shutdown()
  }

  def getValue(key: String): Option[String] = {
    if (System.getenv(key) != null) {
      Some(System.getenv(key))
    } else if (System.getProperty(key) != null) {
      Some(System.getProperty(key))
    } else {
      None
    }
  }
}
