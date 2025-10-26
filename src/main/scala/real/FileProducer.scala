package real

import sttp.client3._
import sttp.client3.okhttp.OkHttpSyncBackend
import sttp.model.MediaType
import spray.json._

object FileProducer extends DefaultJsonProtocol {

  // ======== Modèles JSON ========
  case class TokenResponse(
                            access_token: String,
                            token_type: String,
                            expires_in: Option[Int]
                          )
  implicit val tokenFormat: RootJsonFormat[TokenResponse] = jsonFormat3(TokenResponse)

  case class UploadBody(url: String)
  implicit val uploadBodyFormat: RootJsonFormat[UploadBody] = jsonFormat1(UploadBody)

  // ======== Backend HTTP ========
  private val backend = OkHttpSyncBackend()

  // ======== Méthode 1 : Récupération du token ========
  /**
   * Récupère un token OAuth2 via le flow client_credentials
   *
   * @param tokenUrl URL du serveur d'authentification (ex: https://auth.server.com/oauth2/token)
   * @param clientId Identifiant client
   * @param clientSecret Secret client
   * @param scope Optionnel : scope d’accès
   * @return Option[String] contenant le token si succès
   */
  def getToken(
                tokenUrl: String,
                clientId: String,
                clientSecret: String,
                scope: Option[String] = None
              ): Option[String] = {

    val requestBody = Map(
      "grant_type" -> "client_credentials",
      "client_id" -> clientId,
      "client_secret" -> clientSecret
    ) ++ scope.map("scope" -> _)

    val request = basicRequest
      .post(uri"$tokenUrl")
      .contentType(MediaType.ApplicationXWwwFormUrlencoded)
      .body(requestBody)
      .response(asStringAlways)

    val response = request.send(backend)

    try {
      val json = response.body.parseJson.convertTo[TokenResponse]
      Some(json.access_token)
    } catch {
      case e: Exception =>
        println(s"[Erreur] Impossible de récupérer le token : ${e.getMessage}")
        None
    }
  }

  // ======== Méthode 2 : Upload avec token ========
  /**
   * Appelle une API d'upload sécurisée avec token OAuth2
   *
   * @param uploadUrl URL de l’API d’upload (ex: https://api.server.com/upload)
   * @param accessToken Token OAuth2 valide
   * @param presignedUrl URL présignée à envoyer dans le corps JSON
   * @param producerId Identifiant du producteur
   * @param signature Signature de sécurité
   * @return Réponse brute de l’API
   */
  def uploadFile(
                  uploadUrl: String,
                  accessToken: String,
                  presignedUrl: String,
                  producerId: String,
                  signature: String
                ): Response[String] = {

    val bodyJson = UploadBody(presignedUrl).toJson.compactPrint

    val request = basicRequest
      .post(uri"$uploadUrl?producer-id=$producerId&signature=$signature")
      .header("Authorization", s"Bearer $accessToken")
      .contentType(MediaType.ApplicationJson)
      .body(bodyJson)
      .response(asStringAlways)

    request.send(backend)
  }

  // ======== Exemple d'utilisation ========
  def main(args: Array[String]): Unit = {
    val tokenOpt = getToken(
      tokenUrl = "https://auth.example.com/oauth2/token",
      clientId = "my-client-id",
      clientSecret = "my-client-secret"
    )

    tokenOpt match {
      case Some(token) =>
        val uploadResponse = uploadFile(
          uploadUrl = "https://api.example.com/upload",
          accessToken = token,
          presignedUrl = "https://storage.example.com/presigned-url",
          producerId = "producer-42",
          signature = "mysignature123"
        )

        println(s"Upload status: ${uploadResponse.code}")
        println(s"Response body: ${uploadResponse.body}")

      case None =>
        println("❌ Impossible d’obtenir le token OAuth2")
    }
  }
}
