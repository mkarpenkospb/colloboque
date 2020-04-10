import com.github.ajalt.clikt.core.CliktCommand
import com.github.ajalt.clikt.parameters.options.default
import com.github.ajalt.clikt.parameters.options.option
import com.github.ajalt.clikt.parameters.types.int
import io.ktor.client.*
import io.ktor.client.engine.apache.*
import io.ktor.client.request.*
import io.ktor.client.statement.*
import io.ktor.http.*
import kotlinx.coroutines.*
import java.io.*
import java.net.*
import java.io.File


class ColloboqueClient : CliktCommand() {

    private val user by option("--pg-user", help="User name").default("postgres")
    private val password by option("--pg-password", help="User password").default("123")
    private val databaseLocal by option("--h2-database", help="Path to client h2 database")
    private val tableH2 by option("--table-h2", help="Name of new table in client database")
    private val tablePsql by option("--table-psql", help="Name of table in server postgres database")
    private val serverPort by option("--server-port", help="Number of the server port").int().default(8080)
    private val serverIp by option("--server-ip", help="Server address").default("127.0.0.1")

    override fun run() {

        val client = HttpClient(Apache) {
            followRedirects = true
        }

        /* Probably two different programmes?*/
        loadTableFromServer(client, serverIp, serverPort, user, password,
                tablePsql ?: throw IllegalArgumentException("Table name expected"),
                tableH2 ?: throw IllegalArgumentException("Table name expected"),
                databaseLocal ?: throw IllegalArgumentException("Local database name expected"))


//        updateTableOnServer(client, serverIp, serverPort, user, password)
    }

}

fun loadTableFromServer(client: HttpClient, ip : String, serverPort : Int, user : String,
                        password : String, table : String, tableH2: String, databaseLocal: String) {
    runBlocking {
        val url: String = "http://$ip:$serverPort/table?user=$user&password=$password&table=$table"

        val localUrl = "jdbc:h2:$databaseLocal"

        importTable(localUrl, tableH2, client.getAsTempFile(url))
    }
}

fun updateTableOnServer(client: HttpClient, ip : String, serverPort : Int, user : String, password : String) {
    runBlocking {
        val url = "http://$ip:$serverPort/update?user=$user&password=$password"
        val queries = UpdateServerDatabase()
        sendPostUpdate(url, queries, client)
    }
}


suspend fun sendPostUpdate(url: String, queries: String, client: HttpClient) {
    val call = client.post<String>(url) {
        body = queries
    }
}



data class HttpClientException(val response: HttpResponse) : IOException("HTTP Error ${response.status}")

suspend fun HttpClient.getAsTempFile(url: String): ByteArray {
    val fileByteArray = ByteArrayOutputStream()
    val response = request<HttpResponse> {
        url(URL(url))
        method = HttpMethod.Get
    }
    if (!response.status.isSuccess()) {
        throw HttpClientException(response)
    }

    fileByteArray.writeBytes(response.readBytes())
    return fileByteArray.toByteArray()
}

fun main(args: Array<String>) = ColloboqueClient().main(args)