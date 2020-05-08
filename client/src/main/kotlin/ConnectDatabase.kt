import java.sql.DriverManager
import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import io.ktor.client.HttpClient
import io.ktor.client.request.post
import kotlinx.coroutines.runBlocking
import com.fasterxml.jackson.module.kotlin.readValue
import org.apache.commons.codec.binary.Base64
import java.sql.Connection
import java.sql.DatabaseMetaData


data class ReplicationResponse(val csvbase64: ByteArray, val sync_num: Int)
data class UpdateRequest(val statements: List<String>, val sync_num: Int)


fun importTable(connectionUrl: String, tableName: String, tableData: ByteArray) {

    val update: ReplicationResponse = jacksonObjectMapper().readValue(tableData)

    val tmp = createTempFile()
    tmp.writeText(String(Base64.decodeBase64(update.csvbase64)))

    DriverManager.getConnection(connectionUrl).use { conn ->
        conn.autoCommit = false
        conn.createStatement().use { stmt ->
            val sql =
                    """
                     CREATE TABLE IF NOT EXISTS $tableName AS SELECT * FROM CSVREAD('${tmp.absolutePath}');
                    """
            stmt.executeUpdate(sql)
        }
        updateSyncNum(conn, update.sync_num)
        conn.commit()
        conn.autoCommit = true
    }

    tmp.delete()
}

fun applyQueries(connectionUrl: String, query: List<String>, clientLog: Log) {

    val queries = mutableListOf<String>()
    DriverManager.getConnection(connectionUrl).use { conn ->
        conn.autoCommit = false
        conn.createStatement().use { stmt ->
            for (sql in query) {
                stmt.executeUpdate(sql)
                queries.add(sql)
            }
        }
        clientLog.writeLog(conn, queries)
        conn.commit()
        conn.autoCommit = true
    }
}


// expected queries as a kind of parameter
fun updateServer(urlServer: String, urlLocal: String, client: HttpClient): Int {

    val queries = mutableListOf<String>()
    var idToDelete = 0

    DriverManager.getConnection(urlLocal).use { conn ->
        conn.createStatement().use { stmt ->
            stmt.executeQuery("SELECT id, sql_command FROM LOG ORDER BY id;").use { res ->
                while (res.next()) {
                    idToDelete = res.getString(1).toInt()
                    queries.add(res.getString(2))
                }
            }
        }
    }

    val response = runBlocking {
        client.post<String>(urlServer) {
            body = jacksonObjectMapper().writeValueAsString(
                    UpdateRequest(queries, getSyncNum(urlLocal))
            )
        }.toInt()
    }

    updateSyncNum(DriverManager.getConnection(urlLocal), response)

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

fun getUserId(connectionUrl: String): String {
    DriverManager.getConnection(connectionUrl).use { conn ->
        conn.createStatement().use { stmt ->
            stmt.executeQuery("SELECT uuid FROM USER_ID WHERE id=0;").use { res ->
                res.next()
                return res.getString(1)
            }
        }
    }
}