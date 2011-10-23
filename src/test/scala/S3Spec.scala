import org.specs._
import dispatch._
import s3._
import S3._
import java.io.{File,FileWriter}

object S3Spec extends Specification {

  val UTF_8 = "UTF-8"
  val test = Bucket("databinder-dispatch-s3-test-bucket")
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
