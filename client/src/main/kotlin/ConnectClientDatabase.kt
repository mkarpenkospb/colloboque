import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import com.fasterxml.jackson.module.kotlin.readValue
import io.ktor.client.request.post
import kotlinx.coroutines.runBlocking
import org.apache.commons.codec.binary.Base64
import java.sql.Connection
import java.sql.DriverManager


fun importTable(client: Client, tableName: String, tableData: ByteArray) {

    val update: ReplicationResponse = jacksonObjectMapper().readValue(tableData)
    val tmp = createTempFile()
    tmp.writeText(String(Base64.decodeBase64(update.csvbase64)))
    client.txnManager.transaction { conn ->
        conn.createStatement().use { stmt ->
            stmt.execute("DROP TABLE IF EXISTS $tableName")
            val sql =
                    """
                     CREATE TABLE IF NOT EXISTS $tableName AS SELECT * FROM CSVREAD('${tmp.absolutePath}');
                    """
            stmt.executeUpdate(sql)
        }
        updateSyncNum(conn, update.sync_num)
    }

    tmp.delete()
}

// expected queries as a kind of parameter
fun updateServer(serverHost: String, serverPort: Int, client: Client): Int {
    val queries = mutableListOf<String>()
    var idToDelete = 0
    DriverManager.getConnection(client.connectionUrl).use { conn ->
        conn.createStatement().use { stmt ->
            stmt.executeQuery("SELECT id, sql_command FROM LOG ORDER BY id;").use { res ->
                while (res.next()) {
                    idToDelete = res.getString(1).toInt()
                    queries.add(res.getString(2))
                }
            }
        }
    }

    var response: Int

    response = runBlocking {
        client.httpClient.post<String>("http://$serverHost:$serverPort/update") {
            body = jacksonObjectMapper().writeValueAsString(UpdateRequest(
                    queries,
                    DriverManager.getConnection(client.connectionUrl).use {conn -> getSyncNum(conn)},
                    client.userId))
        }.toInt()
    }

    // response -2 means client is behind
    if (response == -2) {
        response = runBlocking {
            client.httpClient.post<String>("http://$serverHost:$serverPort/merge") {
                body = jacksonObjectMapper().writeValueAsBytes(
                        MergeRequest(
                                queries,
                                Base64.encodeBase64(
                                        DriverManager.getConnection(client.connectionUrl).use {
                                            conn -> loadTableFromDB(conn, "MAIN_TABLE_COPY") }),
                                DriverManager.getConnection(client.connectionUrl).use { conn -> getSyncNum(conn) },
                                client.userId))
            }.toInt()
        }
        loadTableFromServer(client, serverHost, serverPort, "TABLE2")
    }

    updateSyncNum(DriverManager.getConnection(client.connectionUrl), response)

    return idToDelete;
}

class TransactionManager(private val connectionUrl: String) {
    fun <T> transaction(code: (Connection) -> T): T {
        return DriverManager.getConnection(connectionUrl).use { conn ->
            conn.autoCommit = false
            code(conn).also {
                conn.commit()
                conn.autoCommit = true
            }
        }
    }
}

