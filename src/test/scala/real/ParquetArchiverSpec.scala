
package real

import org.scalatest.wordspec.AnyWordSpec
import org.scalatest.matchers.should.Matchers

class ParquetArchiverSpec extends AnyWordSpec with Matchers {
  "ParquetArchiver" should {
    "compile and expose simple API" in {
      // API smoke checks (no execution against HDFS here)
      noException should be thrownBy {
        ParquetArchiver.writeTmp _
        ParquetArchiver.appendPartitions _
        ParquetArchiver.overwriteWithTmp _
        ParquetArchiver.createTarFromTmp _
        ParquetArchiver.cleanTmpExceptTar _
      }
    }
  }
}
