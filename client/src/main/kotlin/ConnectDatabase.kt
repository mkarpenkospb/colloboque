import java.sql.DriverManager
import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import io.ktor.client.HttpClient
//import io.ktor.client.request.post
//import io.ktor.client.request.forms
import kotlinx.coroutines.runBlocking
import com.fasterxml.jackson.module.kotlin.readValue
import io.ktor.client.request.post
import org.apache.commons.codec.binary.Base64
import java.sql.Connection


data class ReplicationResponse(val csvbase64: ByteArray, val sync_num: Int)
data class UpdateRequest(val statements: List<String>, val sync_num: Int)


fun importTable(client: Client, tableName: String, tableData: ByteArray) {

    val update: ReplicationResponse = jacksonObjectMapper().readValue(tableData)

    val tmp = createTempFile()
    tmp.writeText(String(Base64.decodeBase64(update.csvbase64)))
    transaction(client) { conn ->
        conn.createStatement().use { stmt ->
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

fun applyQueries(client: Client, query: List<String>) {

    val queries = mutableListOf<String>()
    transaction(client) { conn ->
        conn.createStatement().use { stmt ->
            for (sql in query) {
                stmt.executeUpdate(sql)
                queries.add(sql)
            }
        }
        client.LOG.writeLog(conn, queries)
    }
}


// expected queries as a kind of parameter
fun updateServer(urlServer: String, client: Client): Int {

    val queries = mutableListOf<String>()
    var idToDelete = 0

    DriverManager.getConnection(client.CONNECTION_URL).use { conn ->
        conn.createStatement().use { stmt ->
            stmt.executeQuery("SELECT id, sql_command FROM LOG ORDER BY id;").use { res ->
                while (res.next()) {
                    idToDelete = res.getString(1).toInt()
                    queries.add(res.getString(2))
                }
            }
        }
    }

    // it doesn't work, maybe server doesn't recognise it as POST, but submitForm POST by default
//    val response = runBlocking {
//        client.HTTP_CLIENT.submitForm<HttpResponse>(
//                url = urlServer,
//                formParameters = Parameters.build {
//                    append("user", client.USER_ID)
//                    append("queries", jacksonObjectMapper().writeValueAsString(
//                    UpdateRequest(queries, getSyncNum(client.CONNECTION_URL))))
//                },
//                encodeInQuery = true
//        )
//    }.toString().toInt()

    val response = runBlocking {
        client.HTTP_CLIENT.post<String>(urlServer) {
            body = jacksonObjectMapper().writeValueAsString(
                    UpdateRequest(queries, getSyncNum(client.CONNECTION_URL))
            )
        }.toInt()
    }

    updateSyncNum(DriverManager.getConnection(client.CONNECTION_URL), response)

    return idToDelete;
}


fun updateSyncNum(conn: Connection, syncNum: Int) {
    conn.prepareStatement("UPDATE SYNCHRONISATION SET sync_num=?").use { stmt ->
        stmt.setInt(1, syncNum)
        stmt.execute()
    }
}

fun getSyncNum(connectionUrl: String): Int {
    DriverManager.getConnection(connectionUrl).use { conn ->
        conn.createStatement().use { stmt ->
            stmt.executeQuery("SELECT sync_num FROM SYNCHRONISATION WHERE id=0;").use { res ->
                res.next()
                return res.getInt(1)
            }
        }
    }
}

fun existsTable(conn: Connection, tableName: String): Boolean {
    val rs = conn.getMetaData().getTables(null, null, tableName, null)
    while (rs.next()) {
        return true
    }
    return false
}

fun <T> transaction(client: Client, code: (Connection) -> T): T {
    return DriverManager.getConnection(client.CONNECTION_URL).use { conn ->
        conn.autoCommit = false
        code(conn).also {
            conn.commit()
            conn.autoCommit = true
        }
    }
}