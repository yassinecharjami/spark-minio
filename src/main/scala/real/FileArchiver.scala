package real

import org.apache.commons.compress.archivers.tar.{TarArchiveEntry, TarArchiveOutputStream}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileStatus, FileSystem, Path}

import java.io.{BufferedOutputStream, File}
import scala.collection.mutable
import java.nio.file.{Paths, Path => NioPath}
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

object FileArchiver extends App {


  val targetPath = new Path("src/main/resources/test")

  archiveFromTmp(targetPath, new Configuration())

  def archiveFromTmp(targetPath: Path, conf: Configuration): Path = {
    val fs = FileSystem.get(conf)

    if (!fs.exists(targetPath)) throw new IllegalArgumentException(s"path ${targetPath.toString} does not exist")

    // On cherche la (ou les) partitions dans le répertoire
    val partitions = fs.listStatus(targetPath).filter(_.isDirectory)
    if (partitions.isEmpty)
      throw new IllegalStateException(s"Aucune partition trouvée sous ${targetPath.toString}")

    // 👉 Ici on prend la première partition (ou la plus récente si besoin)
    val latestPartition = partitions.maxBy(_.getModificationTime).getPath

    val allFiles: Seq[FileStatus] = listAllRecursive(fs, latestPartition).filterNot(_.getPath.getName.endsWith(".tar"))

    val timestamp = DateTimeFormatter.ofPattern("yyyyMMddHHmmss").format(LocalDateTime.now())

    val tarPath = new Path(s"${latestPartition.getParent}/$timestamp.tar")

    val tarOut: TarArchiveOutputStream = new TarArchiveOutputStream(
      new BufferedOutputStream(fs.create(tarPath, true))
    )

    tarOut.setLongFileMode(TarArchiveOutputStream.LONGFILE_GNU)

    allFiles.foreach { status: FileStatus =>

      val relativePath: String = getRelativePath(latestPartition, status.getPath)

      val entry = new TarArchiveEntry(relativePath)

      if (!status.isDirectory) {
        entry.setSize(status.getLen)
        tarOut.putArchiveEntry(entry)
        val in = fs.open(status.getPath)
        val buffer = new Array[Byte](8096)
        Iterator
          .continually(in.read(buffer))
          .takeWhile(_ != -1)
          .foreach(read => tarOut.write(buffer, 0, read))
        in.close()
        tarOut.closeArchiveEntry()
      }
    }

    tarOut.finish()
    tarOut.close()
    tarPath
  }


  def listAllRecursive(fs: FileSystem, path: Path): Seq[FileStatus] = {
    val files = mutable.Buffer[FileStatus]()
    if (fs.exists(path)) {
      val iter: Array[FileStatus] = fs.listStatus(path)
      iter.foreach { f =>
        files += f
        if (f.isDirectory) files ++= listAllRecursive(fs, f.getPath)
      }
    }
    files.toSeq
  }

  def getRelativePath(base: Path, full: Path): String = {
    // On supprime "file:" s'il existe
    val baseClean = base.toUri.getPath
    val fullClean = full.toUri.getPath

    // On convertit en chemins système (java.nio)
    val baseNio: NioPath = Paths.get(baseClean).normalize().toAbsolutePath
    val fullNio: NioPath = Paths.get(fullClean).normalize().toAbsolutePath

    // On relativise
    val rel = baseNio.relativize(fullNio).toString.replace("\\", "/")
    rel
  }
}
