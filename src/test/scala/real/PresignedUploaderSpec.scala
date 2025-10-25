
package real

import org.scalatest.wordspec.AnyWordSpec
import org.scalatest.matchers.should.Matchers
import sttp.client3._
import sttp.client3.testing.SttpBackendStub
import sttp.model.Method

import java.io.{File, PrintWriter}

class PresignedUploaderSpec extends AnyWordSpec with Matchers {
  "PresignedUploader.upload" should {
    "perform PUT to presigned URL" in {
      val f = File.createTempFile("upwwwwwwwwwww", ".bin")
      val pw = new PrintWriter(f); pw.write("hello"); pw.close()

      implicit val backend: SttpBackendStub[Identity, Any] =
        SttpBackendStub.synchronous
          .whenRequestMatches(_.method == Method.PUT)
          .thenRespond("OK")

      val resp = basicRequest.put(uri"https://example/presigned").body(f).send(backend)
      resp.code.code shouldBe 200

      f.delete()
    }
  }
}
