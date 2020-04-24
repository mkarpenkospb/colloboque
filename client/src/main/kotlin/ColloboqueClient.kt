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
import java.net.*

class ColloboqueClient : CliktCommand() {

    private val databaseLocal by option("--h2-database", help = "Path to client h2 database")
    private val h2Table by option("--h2-table", help = "Name of new table in client database").default("BaseTable")
    private val pgTable by option("--pg-table", help = "Name of table in server postgres database").default("BaseTable")
    private val serverPort by option("--server-port", help = "Number of the server port").int().default(8080)
    private val serverHost by option("--server-host", help = "Server address").default("localhost")


    val createLogTable = """CREATE TABLE IF NOT EXISTS LOG(
                            |id bigint auto_increment, 
                            |sql_command TEXT NOT NULL);""".trimMargin()


    override fun run() {
        val clientLog = Log("jdbc:h2:$databaseLocal", createLogTable)
        val client = HttpClient(Apache) {
            followRedirects = true
        }

        /* Probably two different programmes?*/
//        loadTableFromServer(client, serverHost, serverPort, pgTable, h2Table,
//                databaseLocal ?: throw IllegalArgumentException("Local database name expected"))

        actionSimulation(client, serverHost, serverPort,
                databaseLocal ?: throw IllegalArgumentException("Local database name expected"),
                clientLog)

//        updateTableOnServer(client, serverHost, serverPort)
    }

}

fun actionSimulation(client: HttpClient, serverHost: String, serverPort: Int,
                     databaseLocal: String, clientLog: Log) {

    val queries = listOf(
            "INSERT INTO table2 (id, first, last, age) VALUES (87, 'Kate', 'Pirson', 19);",
            "INSERT INTO table2 (id, first, last, age) VALUES (88, 'Anna', 'Pirson', 199);",
            "INSERT INTO table2 (id, first, last, age) VALUES (89, 'Mary', 'Pirson', 20);"
    )

    applyQueries("jdbc:h2:$databaseLocal", queries, clientLog)

    clientLog.clear(updateServer("http://$serverHost:$serverPort/update",
            "jdbc:h2:$databaseLocal", client))
}


fun loadTableFromServer(client: HttpClient, serverHost: String, serverPort: Int,
                        table: String, h2Table: String, databaseLocal: String) {
    
    runBlocking {
        importTable("jdbc:h2:$databaseLocal", h2Table,
                client.getAsTempFile("http://$serverHost:$serverPort/table?table=$table"))
    }

}



suspend fun sendPostUpdate(url: String, queries: String, client: HttpClient) {

    client.post<String>(url) {
        body = queries
    }

}


suspend fun HttpClient.getAsTempFile(url: String): ByteArray {

    val response = request<HttpResponse> {
        url(URL(url))
        method = HttpMethod.Get
    }

    if (!response.status.isSuccess()) {
        throw RuntimeException("response status fail")
    }

    return response.readBytes()
}

fun main(args: Array<String>) = ColloboqueClient().main(args)