package real

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import sttp.client3._
import sttp.client3.okhttp.OkHttpSyncBackend
import sttp.model.MediaType

import java.io.{File, IOException, InputStream}
import scala.util.{Failure, Success, Try}

object UploadFromHdfs {

  private val bufferSize = 8 * 1024 // 8 KB

  /**
   * Upload d'un fichier présent sur HDFS vers une URL présignée PUT.
   * 1️⃣ Tente un upload en streaming direct HDFS → PUT
   * 2️⃣ Si échec, fallback vers copie locale /tmp + upload
   */
  def upload(fs: FileSystem, hdfsPath: Path, presignedUrl: String): Unit = {
    println(s"🔹 Tentative d’upload en streaming depuis HDFS: ${hdfsPath.toString}")
    Try(uploadStream(fs, hdfsPath, presignedUrl)) match {
      case Success(_) =>
        println(s"✅ Upload réussi en streaming depuis HDFS: ${hdfsPath.getName}")

      case Failure(e) =>
        println(s"⚠️ Échec du streaming direct (${e.getMessage}), fallback vers copie locale...")
        Try(uploadViaTmp(fs, hdfsPath, presignedUrl)) match {
          case Success(_) =>
            println(s"✅ Upload réussi après fallback depuis /tmp pour ${hdfsPath.getName}")
          case Failure(e2) =>
            println(s"❌ Upload échoué même après fallback : ${e2.getMessage}")
            throw e2
        }
    }
  }

  /**
   * Méthode 1 : upload en streaming direct depuis HDFS vers l’URL PUT
   */
  private def uploadStream(fs: FileSystem, hdfsPath: Path, presignedUrl: String): Unit = {
    implicit val backend = OkHttpSyncBackend()
    val in: InputStream = fs.open(hdfsPath)

    val TarMediaType: MediaType =
      MediaType.parse("application/x-tar").getOrElse(MediaType.ApplicationOctetStream)

    try {
      val request = basicRequest
        .put(uri"$presignedUrl")
        .contentType("application/x-tar")
        .body(InputStreamBody(in, TarMediaType))
        .readTimeout(scala.concurrent.duration.Duration.Inf) // important pour les gros fichiers

      val response = request.send(backend)

      if (!(response.code.isSuccess))
        throw new IOException(s"Erreur HTTP ${response.code}: ${response.statusText}")

    } finally {
      in.close()
      backend.close()
    }
  }

  /**
   * Méthode 2 : fallback — copie le fichier HDFS vers /tmp, puis upload depuis le local
   */
  private def uploadViaTmp(fs: FileSystem, hdfsPath: Path, presignedUrl: String): Unit = {
    implicit val backend = OkHttpSyncBackend()
    val localFile = new File(s"/tmp/${hdfsPath.getName}")

    try {
      // Copie depuis HDFS
      fs.copyToLocalFile(hdfsPath, new Path(localFile.getAbsolutePath))

      val request = basicRequest
        .put(uri"$presignedUrl")
        .contentType("application/x-tar")
        .body(localFile)
        .readTimeout(scala.concurrent.duration.Duration.Inf)

      val response = request.send(backend)

      if (response.code.isSuccess)
        println(s"✅ Upload réussi depuis /tmp (${localFile.getAbsolutePath})")
      else
        throw new IOException(s"Erreur HTTP ${response.code}: ${response.statusText}")

    } finally {
      backend.close()
      if (localFile.exists()) {
        localFile.delete()
        println(s"🧹 Fichier temporaire supprimé: ${localFile.getAbsolutePath}")
      }
    }
  }

  /*
  // Exemple d’utilisation
  val conf = new Configuration()
  conf.set("fs.defaultFS", "hdfs://namenode:8020") // si besoin
  val fs = FileSystem.get(conf)

  val hdfsPath = new Path("/user/data/archive_20251026.tar")
  val presignedUrl = "https://my-s3-presigned-url.example.com/..." // ton URL PUT

  UploadFromHdfs.upload(fs, hdfsPath, presignedUrl)
   */
}
