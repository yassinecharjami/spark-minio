package real

import org.apache.hadoop.conf.Configuration
import org.apache.spark.sql.SparkSession

import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

object Spark extends App {

  implicit val spark = SparkSession.builder()
              .appName("Playing")
              .master("local[*]")
              .getOrCreate()

  private val serialVersionUID = 7789290765573734431L // Remplacez par la valeur correcte


  import spark.implicits._

  val data = Seq(
    (1, "alice", 30)
  )

  /*
    partitionPath: file:/home/yassinec/Desktop/spark-minio/src/main/resources/tmp/name=alice
  tarPath: file:/home/yassinec/Desktop/spark-minio/src/main/resources/tmp/name=alice/20251109102108.tar
  Archive créée : file:/home/yassinec/Desktop/spark-minio/src/main/resources/tmp/name=alice/20251109102108.tar
                  file:/home/yassinec/Desktop/spark-minio/src/main/resources/tmp/name=alice/20251109102108.tar
   */

  val df = data.toDF("id", "name", "age")

  df.show()

  val timestamp = DateTimeFormatter.ofPattern("yyyyMMddHHmmss").format(LocalDateTime.now())

  val targetTmpPath: String = s"src/main/resources/tmp"
  val targetPath: String = s"src/main/resources/srv"

  val conf = new Configuration()


  val tarPath = ParquetArchiver.process(df, targetTmpPath, targetPath, Option(timestamp))

  val bucket = "spark-bucket"
  val objectKey = tarPath.toString.stripPrefix("file:/").replace("/", "~")

  println("objectKey: " + objectKey)

  // Étape 1 : générer les deux URLs
  val (putUrl, getUrl) = MinioPresignedUrls.generatePresignedUrls(
    bucketName = bucket,
    objectKey = objectKey,
    expirationSeconds = 3600,
    endpoint = "http://localhost:9000",
    accessKey = "accessKey",
    secretKey = "secretKey"
  )

  // Étape 2 : upload du fichier via l’URL PUT
  PresignedUploader.uploadFileWithPresignedPutUrl(putUrl, tarPath.toString)

  println(s"➡️  Tu peux télécharger ton fichier via :\n$getUrl")

  println("Spark Session is running. Press Enter to stop...")
  scala.io.StdIn.readLine() // Attend une entrée utilisateur

  /**
   * Regex pour objectKey:
   * if number of fields is fixed
   *
   * tu mets un nombre largement suffisant de placeholders (8 à 10),
   *
   * s’il n’y a pas autant de champs, les ${n} manquants seront simplement vides ou ignorés (selon l’implémentation).
   *
   * {
   * "inputPattern": null,
   * "outputPattern": "${1}/${2}/${3}/${4}/${5}/${6}/${7}/${8}/${9}/${10}",
   * "fieldSeparator": "~"
   * }
   *
   * else:
   * {
   * "inputPattern": "^(.*)$",
   * "outputPattern": "${fields.join('/')}",
   * "fieldSeparator": "~"
   * }
   *
   * or
   *
   * {
   * "inputPattern": "^(.*)$",
   * "outputPattern": "${1/~/\\/g}",
   * "fieldSeparator": "¤"
   * }
   *
   */

  // Fermer la session Spark après l'entrée
  spark.stop()

}
