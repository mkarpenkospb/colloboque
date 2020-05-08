import com.github.ajalt.clikt.core.CliktCommand
import com.github.ajalt.clikt.parameters.options.default
import com.github.ajalt.clikt.parameters.options.option
import com.github.ajalt.clikt.parameters.types.int
import io.ktor.client.*
import io.ktor.client.engine.apache.*
import io.ktor.client.request.*
import io.ktor.client.statement.*
import io.ktor.http.*
import io.ktor.http.cio.expectMultipart
import kotlinx.coroutines.*
import java.net.*
import java.sql.DriverManager
import java.util.*

var USER_ID : String? = null

class ColloboqueClient : CliktCommand() {

    private val databaseLocal by option("--h2-database", help = "Path to client h2 database")
    private val h2Table by option("--h2-table", help = "Name of new table in client database").default("BaseTable")
    private val pgTable by option("--pg-table", help = "Name of table in server postgres database").default("BaseTable")
    private val serverPort by option("--server-port", help = "Number of the server port").int().default(8080)
    private val serverHost by option("--server-host", help = "Server address").default("localhost")

    // ----------------- SQL queries constants ---------------------------
    private val createLogTable = """CREATE TABLE IF NOT EXISTS LOG(
                                        |id bigint auto_increment, 
                                        |sql_command TEXT NOT NULL);""".trimMargin()

    private val createUserIdTable = """CREATE TABLE USER_ID(
                            |id INT PRIMARY KEY NOT NULL,      
                            |uuid TEXT NOT NULL);
                            """.trimMargin()

    private val insertUID = """INSERT INTO USER_ID(id, uuid) VALUES (0, '%s')"""

    override fun run() {
        // ------------------ create user id if not exists -------------------
        DriverManager.getConnection("jdbc:h2:$databaseLocal").use { conn ->
            conn.autoCommit = false
            if (!existsTable(conn, "USER_ID")) {
                val userId = UUID.randomUUID().toString()
                conn.createStatement().use { stmt ->
                    stmt.execute(createUserIdTable)
                    stmt.execute(insertUID.format(userId))
                }
            }
            conn.commit()
            conn.autoCommit = true
        }

        USER_ID = getUserId("jdbc:h2:$databaseLocal")
        // -----------------------------------------------------------------

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

    }

}

fun actionSimulation(client: HttpClient, serverHost: String, serverPort: Int,
                     databaseLocal: String, clientLog: Log) {

    val queries = listOf(
            "INSERT INTO table2 (id, first, last, age) VALUES (109, 'Kate', 'Pirson', 116);",
            "INSERT INTO table2 (id, first, last, age) VALUES (110, 'Anna', 'Pirson', 117);",
            "INSERT INTO table2 (id, first, last, age) VALUES (111, 'Mary', 'Pirson', 118);"
    )

    applyQueries("jdbc:h2:$databaseLocal", queries, clientLog)

    clientLog.clear(updateServer("http://$serverHost:$serverPort/update?user=$USER_ID",
            "jdbc:h2:$databaseLocal", client))
}


fun loadTableFromServer(client: HttpClient, serverHost: String, serverPort: Int,
                        table: String, h2Table: String, databaseLocal: String) {
    
    runBlocking {
        importTable("jdbc:h2:$databaseLocal", h2Table,
                client.getAsTempFile(
                        "http://$serverHost:$serverPort/table?user=$USER_ID"
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