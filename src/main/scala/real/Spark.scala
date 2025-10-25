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
    (1, "alice", 30),
    (2, "bob", 25),
    (3, "charlie", 28),
    (4, "david", 40),
    (5, "eve", 22),
    (6, "omar", 35)
  )

  val df = data.toDF("id", "name", "age")

  df.show()

  val timestamp = DateTimeFormatter.ofPattern("yyyyMMddHHmmss").format(LocalDateTime.now())

  val targetTmpPath: String = s"src/main/resources/tmp/$timestamp"
  val targetPath: String = s"src/main/resources/srv"

  val conf = new Configuration()


  ParquetArchiver.process(df, targetTmpPath, targetPath, Option(timestamp))

  val tarPath = s"$targetTmpPath/$timestamp"

  val bucket = "spark-bucket"
  val objectKey = s"srv/$timestamp"

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
  PresignedUploader.uploadFileWithPresignedPutUrl(putUrl, tarPath)

  println(s"➡️  Tu peux télécharger ton fichier via :\n$getUrl")

  println("Spark Session is running. Press Enter to stop...")
  scala.io.StdIn.readLine() // Attend une entrée utilisateur

  // Fermer la session Spark après l'entrée
  spark.stop()

}
