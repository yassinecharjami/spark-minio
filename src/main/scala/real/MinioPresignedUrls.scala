package real

import com.amazonaws.HttpMethod
import com.amazonaws.auth.BasicAWSCredentials
import com.amazonaws.client.builder.AwsClientBuilder.EndpointConfiguration
import com.amazonaws.services.s3.AmazonS3ClientBuilder

import java.time.Instant
import java.util.Date

object MinioPresignedUrls {

  /**
   * Génère une URL présignée PUT et GET pour un même objet
   *
   * @param bucketName nom du bucket (ex: "spark-bucket")
   * @param objectKey nom/chemin de l’objet (ex: "archives/archive_20251024_180000.tar")
   * @param expirationSeconds durée de validité en secondes (ex: 3600)
   * @param endpoint MinIO endpoint (ex: "http://localhost:9000")
   * @param accessKey identifiant MinIO
   * @param secretKey secret MinIO
   * @return (putUrl, getUrl)
   */
  def generatePresignedUrls(
                             bucketName: String,
                             objectKey: String,
                             expirationSeconds: Long = 3600,
                             endpoint: String = "http://localhost:9000",
                             accessKey: String = "accessKey",
                             secretKey: String = "secretKey"
                           ): (String, String) = {

    val creds = new BasicAWSCredentials(accessKey, secretKey)
    val s3Client = AmazonS3ClientBuilder.standard()
      .withEndpointConfiguration(new EndpointConfiguration(endpoint, "us-east-1"))
      .withCredentials(new com.amazonaws.auth.AWSStaticCredentialsProvider(creds))
      .withPathStyleAccessEnabled(true)
      .build()

    val expiration = Date.from(Instant.now().plusSeconds(expirationSeconds))

    val putUrl = s3Client.generatePresignedUrl(bucketName, objectKey, expiration, HttpMethod.PUT).toString
    val getUrl = s3Client.generatePresignedUrl(bucketName, objectKey, expiration, HttpMethod.GET).toString

    println(s"🔐 PUT presigned URL (valide ${expirationSeconds}s):\n$putUrl\n")
    println(s"🔓 GET presigned URL (valide ${expirationSeconds}s):\n$getUrl\n")

    (putUrl, getUrl)
  }
}
