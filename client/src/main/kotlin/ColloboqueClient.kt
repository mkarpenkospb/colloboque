import com.github.ajalt.clikt.core.CliktCommand
import com.github.ajalt.clikt.parameters.options.default
import com.github.ajalt.clikt.parameters.options.option
import com.github.ajalt.clikt.parameters.types.int
import io.ktor.client.*
import io.ktor.client.request.*
import io.ktor.client.statement.*
import io.ktor.http.*
import kotlinx.coroutines.*
import java.net.*

class ColloboqueClient : CliktCommand() {

    private val databaseLocal by option("--h2-database", help = "Path to client h2 database")
    private val h2Table by option("--h2-table", help = "Name of new table in client database").default("MAIN_TABLE")
    private val pgTable by option("--pg-table", help = "Name of table in server postgres database").default("MAIN_TABLE")
    private val serverPort by option("--server-port", help = "Number of the server port").int().default(8080)
    private val serverHost by option("--server-host", help = "Server address").default("localhost")


    override fun run() {
        val sessionClient = Client("jdbc:h2:$databaseLocal")

//        loadTableFromServer(sessionClient, serverHost, serverPort, pgTable)

        actionSimulation(sessionClient, serverHost, serverPort)
    }

}

fun actionSimulation(client: Client, serverHost: String, serverPort: Int) {

    val queries = listOf(
            "INSERT INTO MAIN_TABLE (id, first, last, age) VALUES (16, 'Kate', 'Pirson', 25);",
            "INSERT INTO MAIN_TABLE (id, first, last, age) VALUES (17, 'Anna', 'Pirson', 26);",
            "INSERT INTO MAIN_TABLE (id, first, last, age) VALUES (18, 'Mary', 'Pirson', 27);"
    )

    client.cloneTable("MAIN_TABLE")

    client.applyQueries(queries)

    client.log.clear(updateServer(serverHost, serverPort, client))
}


fun loadTableFromServer(client: Client, serverHost: String, serverPort: Int, h2Table: String) {
    
    runBlocking {
        importTable(client, h2Table,
                client.httpClient.getAsTempFile(
                        "http://$serverHost:$serverPort/table?user=${client.userId}"
                )
        )
    }

}


suspend fun HttpClient.getAsTempFile(connectionUrl: String): ByteArray {

    val response = request<HttpResponse> {
        url(URL(connectionUrl))
        method = HttpMethod.Get
    }

    if (!response.status.isSuccess()) {
        throw RuntimeException("response status fail")
    }

    return response.readBytes()
}

fun main(args: Array<String>) = ColloboqueClient().main(args)