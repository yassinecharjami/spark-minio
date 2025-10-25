package real

import org.apache.commons.compress.archivers.tar.{TarArchiveEntry, TarArchiveOutputStream}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileStatus, FileSystem, Path}
import org.apache.spark.sql.{DataFrame, SparkSession}

import java.io.{BufferedOutputStream, InputStream}
import java.net.URI
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter


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
    val files = scala.collection.mutable.Buffer[FileStatus]()
    if (fs.exists(path)) {
      val iter = fs.listStatus(path)
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
  def createTarFromTmp(tmpPath: String, tarName: Option[String], conf: Configuration): Path = {
    val fs: FileSystem = FileSystem.get(new URI(tmpPath), conf)
    val basePath = new Path(tmpPath)

    // Nom du tar
    val timestamp = DateTimeFormatter.ofPattern("yyyyMMddHHmmss").format(LocalDateTime.now())
    val tarFileName = tarName.getOrElse(s"archive_$timestamp.tar")
    val tarPath = new Path(basePath, tarFileName)

    val tarOut = new TarArchiveOutputStream(
      new BufferedOutputStream(fs.create(tarPath, true))
    )
    tarOut.setLongFileMode(TarArchiveOutputStream.LONGFILE_GNU)

    val allFiles = listAllRecursive(fs, basePath)
      .filterNot(_.getPath.getName == tarFileName) // éviter d’ajouter le tar lui-même

    allFiles.foreach { status =>
      val relativePath = basePath.getName + "/" + makeRelativePath(basePath, status.getPath)
      val entry = new TarArchiveEntry(relativePath)

      if (status.isDirectory) {
        tarOut.putArchiveEntry(entry)
        tarOut.closeArchiveEntry()
      } else {
        entry.setSize(status.getLen)
        tarOut.putArchiveEntry(entry)
        val in: InputStream = fs.open(status.getPath)
        val buffer = new Array[Byte](8096)
        var bytesRead = in.read(buffer)
        while (bytesRead != -1) {
          tarOut.write(buffer, 0, bytesRead)
          bytesRead = in.read(buffer)
        }
        in.close()
        tarOut.closeArchiveEntry()
      }
    }

    tarOut.finish()
    tarOut.close()
    tarPath
  }

  /** Déplace uniquement les dossiers de partition (name=xxx) vers la cible Hive */
  def movePartitionDirsToTarget(tmpPath: String, targetPath: String, conf: Configuration): Unit = {
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
      val tarPath = createTarFromTmp(tmpPath, tarName, conf)
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