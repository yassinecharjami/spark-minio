package real

import java.io.{BufferedInputStream, FileInputStream, OutputStream}
import java.net.{HttpURLConnection, URL}

object PresignedUploader {

  /**
   * Upload d’un fichier via une URL présignée PUT
   *
   * @param presignedPutUrl URL PUT générée par MinIO
   * @param filePath chemin local du fichier à envoyer
   */
  def uploadFileWithPresignedPutUrl(presignedPutUrl: String, filePath: String): Unit = {
    val file = new java.io.File(filePath.stripPrefix("file:"))
    if (!file.exists()) throw new IllegalArgumentException(s"Fichier introuvable: $filePath")

    val url = new URL(presignedPutUrl)
    val connection = url.openConnection().asInstanceOf[HttpURLConnection]
    connection.setDoOutput(true)
    connection.setRequestMethod("PUT")
    connection.setRequestProperty("Content-Type", "application/octet-stream")
    connection.setFixedLengthStreamingMode(file.length())

    val out: OutputStream = connection.getOutputStream
    val in = new BufferedInputStream(new FileInputStream(file))
    val buffer = new Array[Byte](8192)
    var bytesRead = in.read(buffer)
    while (bytesRead != -1) {
      out.write(buffer, 0, bytesRead)
      bytesRead = in.read(buffer)
    }
    in.close()
    out.close()

    val responseCode = connection.getResponseCode
    if (responseCode >= 200 && responseCode < 300)
      println(s"✅ Upload réussi (code HTTP: $responseCode)")
    else
      println(s"❌ Échec upload (code HTTP: $responseCode, message: ${connection.getResponseMessage})")

    connection.disconnect()
  }
}
