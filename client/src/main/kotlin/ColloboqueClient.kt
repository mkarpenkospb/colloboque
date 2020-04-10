import com.github.ajalt.clikt.core.CliktCommand
import com.github.ajalt.clikt.parameters.options.default
import com.github.ajalt.clikt.parameters.options.option
import com.github.ajalt.clikt.parameters.types.int
import io.ktor.client.*
import io.ktor.client.engine.apache.*
import io.ktor.client.request.*
import io.ktor.client.statement.*
import io.ktor.http.*
import io.ktor.util.cio.*
import io.ktor.utils.io.*
import kotlinx.coroutines.*
import java.io.*
import java.net.*
import java.io.File


class ColloboqueClient : CliktCommand() {

    private val dbServer by option("--pg-database", help="Name of the database").default("postgres")
    private val user by option("--pg-user", help="User name").default("postgres")
    private val password by option("--pg-password", help="User password").default("123")
    private val dbClient by option("--h2-database", help="Path to client h2 database")
    private val tableH2 by option("--table-h2", help="Name of new table in client database")
    private val tablePsql by option("--table-psql", help="Name of table in server postgres database")
    private val serverPort by option("--server-port", help="Number of the server port").int().default(8080)
    private val serverIp by option("--server-ip", help="Server address").default("127.0.0.1")

    override fun run() {
        val url = "jdbc:h2:~/$dbClient"
        startClient(serverIp, serverPort,dbServer, user, password, tablePsql)
        importTable(url, tableH2, "/tmp/recieved.csv")
    }

}


fun startClient(ip : String, serverPort : Int, dbServer : String,
                user : String, password : String, table : String?) {
    runBlocking {
        val client = HttpClient(Apache) {
            followRedirects = true
        }

        val url: String =
                """http://$ip:$serverPort/table?database=$dbServer&
                user=$user&password=$password&
                table=$table""".filter {c-> !Character.isWhitespace(c) }

        client.getAsTempFile(url) { file ->
            File("/tmp/recieved.csv").writeBytes(file.readBytes())
        }

    }
}

data class HttpClientException(val response: HttpResponse) : IOException("HTTP Error ${response.status}")

suspend fun HttpClient.getAsTempFile(url: String, callback: suspend (file: File) -> Unit) {
    val file = getAsTempFile(url)
    try {
        callback(file)
    } finally {
        file.delete()
    }
}

suspend fun HttpClient.getAsTempFile(url: String): File {
    val file = File("/tmp/client_buffer.csv")
    val response = request<HttpResponse> {
        url(URL(url))
        method = HttpMethod.Get
    }
    if (!response.status.isSuccess()) {
        throw HttpClientException(response)
    }
    response.content.copyAndClose(file.writeChannel())
    return file
}

fun main(args: Array<String>) = ColloboqueClient().main(args)