package real

import com.amazonaws.auth.BasicAWSCredentials
import com.amazonaws.client.builder.AwsClientBuilder.EndpointConfiguration
import com.amazonaws.services.s3.AmazonS3ClientBuilder
import com.amazonaws.services.s3.model.{CreateBucketRequest, PutObjectRequest}

import java.io.File

object MinioUploader {

  /**
   * Upload d’un fichier .tar vers un bucket MinIO (compatible S3)
   *
   * @param tarFilePath chemin local du tar (ex: "src/main/resources/tmp/archive_20251024_180000.tar")
   * @param bucketName nom du bucket MinIO (ex: "spark-bucket")
   * @param objectKey nom/chemin du fichier dans le bucket (ex: "archives/archive_20251024_180000.tar")
   * @param endpoint URL du service MinIO (ex: "http://localhost:9000")
   * @param accessKey clé d’accès MinIO
   * @param secretKey clé secrète MinIO
   */
  def uploadTarToMinio(
                        tarFilePath: String,
                        bucketName: String,
                        objectKey: String,
                        endpoint: String = "http://localhost:9000",
                        accessKey: String = "accessKey",
                        secretKey: String = "secretKey"
                      ): Unit = {

    val credentials = new BasicAWSCredentials(accessKey, secretKey)

    val s3Client = AmazonS3ClientBuilder.standard()
      .withEndpointConfiguration(new EndpointConfiguration(endpoint, "us-east-1"))
      .withCredentials(new com.amazonaws.auth.AWSStaticCredentialsProvider(credentials))
      .withPathStyleAccessEnabled(true) // obligatoire pour MinIO / Scality / Ceph
      .build()

    val tarFile = new File(tarFilePath)
    if (!tarFile.exists())
      throw new IllegalArgumentException(s"Fichier TAR introuvable: $tarFilePath")

    // Création du bucket si inexistant
    if (!s3Client.doesBucketExistV2(bucketName)) {
      println(s"🪣 Création du bucket '$bucketName' ...")
      s3Client.createBucket(new CreateBucketRequest(bucketName))
    }

    println(s"⬆️  Upload du fichier ${tarFile.getName} vers s3://$bucketName/$objectKey ...")

    val putRequest = new PutObjectRequest(bucketName, objectKey, tarFile)
    s3Client.putObject(putRequest)

    println(s"✅ Upload terminé avec succès : s3://$bucketName/$objectKey")
  }
}