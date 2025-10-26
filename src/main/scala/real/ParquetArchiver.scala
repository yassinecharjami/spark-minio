package real

import org.apache.commons.compress.archivers.tar.{TarArchiveEntry, TarArchiveOutputStream}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileStatus, FileSystem, Path}
import org.apache.spark.sql.{DataFrame, SparkSession}

import java.io.BufferedOutputStream
import java.net.URI
import java.nio.file.Paths
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter
import scala.collection.mutable
import java.nio.file.{Path => NioPath}


object ParquetArchiver {

  /** Écrit le DataFrame partitionné dans un répertoire temporaire */
  def writeTmp(df: DataFrame, tmpPath: String): Unit = {
    df.write
      .partitionBy("name")
      .mode("overwrite")
      .parquet(tmpPath)
  }

  /** Liste récursivement tous les fichiers et dossiers */
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

  /** Retourne le chemin relatif entre base et file */
  def makeRelativePath(base: Path, file: Path): String = {
    val baseUri = base.toUri.getPath.stripSuffix("/")
    val fileUri = file.toUri.getPath
    fileUri.stripPrefix(baseUri + "/")
  }

  /** Archive tout le contenu du dossier tmp (fichiers + dossiers) */
  def createTarFromTmp(targetPath: Path, conf: Configuration): Path = {
    val fs = FileSystem.get(conf)

    if (!fs.exists(targetPath)) throw new IllegalArgumentException(s"path ${targetPath.toString} does not exist")

    val allFiles: Seq[FileStatus] = listAllRecursive(fs, targetPath).filterNot(_.getPath.getName.endsWith(".tar"))

    val timestamp = DateTimeFormatter.ofPattern("yyyyMMddHHmmss").format(LocalDateTime.now())

    val tarPath = new Path(s"$targetPath/$timestamp.tar")

    val tarOut: TarArchiveOutputStream = new TarArchiveOutputStream(
      new BufferedOutputStream(fs.create(tarPath, true))
    )

    tarOut.setLongFileMode(TarArchiveOutputStream.LONGFILE_GNU)

    allFiles.foreach { status: FileStatus =>

      val relativePath: String = getRelativePath(targetPath, status.getPath)

      val entry = new TarArchiveEntry(relativePath)

      if (status.isDirectory) {
        tarOut.putArchiveEntry(entry)
        tarOut.closeArchiveEntry()
      } else {
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

  /** Déplace uniquement les dossiers de partition (name=xxx) vers la cible Hive */
  def appendPartitionDirsToTarget(tmpPath: String, targetPath: String, conf: Configuration): Unit = {
    val fs = FileSystem.get(new URI(tmpPath), conf)
    val srcPath = new Path(tmpPath)
    val dstPath = new Path(targetPath)
    if (!fs.exists(dstPath)) fs.mkdirs(dstPath)

    val elements = fs.listStatus(srcPath)
    elements.foreach { el =>
      val name = el.getPath.getName
      // déplace seulement les répertoires de partition
      if (el.isDirectory && name.contains("=")) {
        val dest = new Path(dstPath, name)
        fs.rename(el.getPath, dest)
      }
    }
  }

  /** Overwrite complet du dossier cible avec les partitions depuis tmp */
  def overwriteTargetWithTmpPartitions(tmpPath: String, targetPath: String, conf: Configuration): Unit = {
    val fs = FileSystem.get(new URI(tmpPath), conf)
    val srcPath = new Path(tmpPath)
    val dstPath = new Path(targetPath)

    // Supprime complètement le dossier cible (sécurisé)
    if (fs.exists(dstPath)) {
      println(s"⚠️  Suppression du contenu existant dans $targetPath ...")
      fs.delete(dstPath, true)
    }
    fs.mkdirs(dstPath)

    // Copie uniquement les dossiers de partition depuis tmp
    val elements = fs.listStatus(srcPath)
    elements.foreach { el =>
      val name = el.getPath.getName
      if (el.isDirectory && name.contains("=")) {
        val dest = new Path(dstPath, name)
        fs.rename(el.getPath, dest)
      }
    }

    println(s"✅  Overwrite effectué : contenu de $targetPath remplacé avec les partitions de $tmpPath")
  }

  /** Supprime tous les fichiers sauf ceux qui se terminent par .tar */
  def cleanTmpExceptTar(tmpPath: String, conf: Configuration): Unit = {
    val fs = FileSystem.get(new URI(tmpPath), conf)
    val base = new Path(tmpPath)
    if (!fs.exists(base)) return

    val elements = fs.listStatus(base)
    elements.foreach { el =>
      if (!el.getPath.getName.endsWith(".tar")) {
        fs.delete(el.getPath, true)
      }
    }
  }

  /** Supprime récursivement un dossier */
  def cleanDirectory(path: String, conf: Configuration): Unit = {
    val fs = FileSystem.get(new URI(path), conf)
    val dir = new Path(path)
    if (fs.exists(dir)) fs.delete(dir, true)
  }

  /** Workflow complet */
  def process(df: DataFrame, tmpPath: String, targetPath: String, tarName: Option[String] = None)
             (implicit spark: SparkSession): Unit = {

    val conf = spark.sparkContext.hadoopConfiguration

    try {
      println("➡️  Écriture des fichiers temporaires...")
      writeTmp(df, tmpPath)

      println("🗜️  Création de l’archive tar (avec partitions)...")
      val tarPath = createTarFromTmp(new Path(tmpPath), conf)
      println(s"✅  Archive créée : $tarPath")

      println("🚚  Déplacement des fichiers/partitions vers la cible Hive...")
      overwriteTargetWithTmpPartitions(tmpPath, targetPath, conf)

      println("🧹  Nettoyage du répertoire temporaire...")
      //cleanTmpExceptTar(tmpPath, conf)

      println("✅  Succès total : fichiers déplacés, archive créée et tmp nettoyé.")
    } catch {
      case e: Exception =>
        println(s"❌  Erreur lors du traitement : ${e.getMessage}")
        e.printStackTrace()
    }
  }
}