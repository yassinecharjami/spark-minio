package real

import org.scalatest.wordspec.AnyWordSpec
import org.scalatest.matchers.should.Matchers
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path, FSDataOutputStream, FileStatus}
import org.apache.hadoop.hdfs.MiniDFSCluster
import java.io.File
import java.nio.file.Files

class ParquetArchiverSpec extends AnyWordSpec with Matchers {

  // Build a MiniDFSCluster before each test suite
  private val baseDir: File = Files.createTempDirectory("hdfs-test").toFile
  private val conf = new Configuration()
  conf.set(MiniDFSCluster.HDFS_MINIDFS_BASEDIR, baseDir.getAbsolutePath)
  private val cluster = new MiniDFSCluster.Builder(conf).numDataNodes(1).build()
  private val fs: FileSystem = cluster.getFileSystem

  private val tmpPath = new Path("/tmp/data")
  private val targetPath = new Path("/data/target")

  // Create temporary test data inside HDFS
  private def createPartitionStructure(): Unit = {
    fs.mkdirs(new Path(tmpPath, "date=2025-10-25"))
    fs.mkdirs(new Path(tmpPath, "country=FR"))
    fs.create(new Path(tmpPath, "non_partition_file.txt")).close()
  }

  // Clean before each test
  private def cleanPaths(): Unit = {
    if (fs.exists(tmpPath)) fs.delete(tmpPath, true)
    if (fs.exists(targetPath)) fs.delete(targetPath, true)
  }

  "ParquetArchiver.appendPartitionDirsToTarget" should {
    "move only directories containing '=' to target" in {
      cleanPaths()
      createPartitionStructure()

      ParquetArchiver.appendPartitionDirsToTarget(tmpPath.toString, targetPath.toString, conf)

      // Check target directory exists and only partition folders moved
      fs.exists(new Path(targetPath, "date=2025-10-25")) shouldBe true
      fs.exists(new Path(targetPath, "country=FR")) shouldBe true
      fs.exists(new Path(targetPath, "non_partition_file.txt")) shouldBe false
    }
  }

  "ParquetArchiver.overwriteTargetWithTmpPartitions" should {
    "overwrite existing target and copy partition folders" in {
      cleanPaths()
      createPartitionStructure()
      fs.mkdirs(new Path(targetPath, "old_data"))

      ParquetArchiver.overwriteTargetWithTmpPartitions(tmpPath.toString, targetPath.toString, conf)

      // Old folder should be gone
      fs.exists(new Path(targetPath, "old_data")) shouldBe false
      // Only partition folders copied
      fs.exists(new Path(targetPath, "date=2025-10-25")) shouldBe true
      fs.exists(new Path(targetPath, "country=FR")) shouldBe true
    }

    "create target folder if it does not exist" in {
      cleanPaths()
      createPartitionStructure()

      ParquetArchiver.overwriteTargetWithTmpPartitions(tmpPath.toString, targetPath.toString, conf)

      fs.exists(targetPath) shouldBe true
    }
  }

  "ParquetArchiver.createTarFromTmp" should {
    "create a tar archive containing all files and folders" in {
      cleanPaths()
      createPartitionStructure()

      // Create one small file in a partition
      val testFilePath = new Path(tmpPath, "date=2025-10-25/test.txt")
      val out: FSDataOutputStream = fs.create(testFilePath)
      out.writeUTF("Hello, world!")
      out.close()

      val tarPath = ParquetArchiver.createTarFromTmp(tmpPath, conf)
      fs.exists(tarPath) shouldBe true

      // Tar file name should contain timestamp
      tarPath.getName should endWith(".tar")
    }

    "throw IllegalArgumentException when target path does not exist" in {
      cleanPaths()
      val invalidPath = new Path("/invalid/path")

      intercept[IllegalArgumentException] {
        ParquetArchiver.createTarFromTmp(invalidPath, conf)
      }
    }
  }

  // Shutdown MiniDFSCluster after all tests
  override def finalize(): Unit = {
    cluster.shutdown()
    super.finalize()
  }
}
