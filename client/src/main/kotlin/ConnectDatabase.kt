import java.sql.DriverManager
import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import io.ktor.client.HttpClient
import io.ktor.client.request.post
import kotlinx.coroutines.runBlocking
import com.fasterxml.jackson.module.kotlin.readValue

fun importTable(connectionUrl: String, tableName: String, tableData: ByteArray) {

    val update: ReplicationPost = jacksonObjectMapper().readValue(tableData)

    val tmp = createTempFile()
    tmp.writeText(update.csvbase64)

    DriverManager.getConnection(connectionUrl).use { conn ->
        conn.createStatement().use { stmt ->
            val sql =
                    """
                     CREATE TABLE IF NOT EXISTS $tableName AS SELECT * FROM CSVREAD('$tmp');
                    """
            stmt.executeUpdate(sql)
        }
    }

    updateSyncNum(connectionUrl, update.sync_num)

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

data class ReplicationPost(val csvbase64: String, val sync_num: Int)
data class UpdatePost(val statements: List<String>, val sync_num: Int)

// expected queries as a kind of parameter
fun updateServer(urlServer: String, urlLocal: String, client: HttpClient): Int {

    val queries = mutableListOf<String>()
    var idToDelete = 0

    DriverManager.getConnection(urlLocal).use { conn ->
        conn.createStatement().use { stmt ->
            stmt.executeQuery("SELECT ID, SQL_COMMAND FROM LOG ORDER BY ID;").use { res ->
                while (res.next()) {
                    idToDelete = res.getString(1).toInt()
                    queries.add(res.getString(2))
                }
            }
        }
    }

    var response : Int? = null

    runBlocking {
        response = client.post<String>(urlServer) {
            body = jacksonObjectMapper().writeValueAsString(
                    UpdatePost(queries,
                            getSyncNum(urlLocal) ?: throw IllegalArgumentException(
                                    "Number of synchronization is not initialised"
                            ))
            )
        }.toInt()
    }

    updateSyncNum(urlLocal, response ?: throw IllegalArgumentException(
            "Number of synchronization is not initialised"
    ))

    return idToDelete;

}


fun updateSyncNum(url: String, syncNum: Int) {

    DriverManager.getConnection(url).use { conn ->
        conn.prepareStatement("UPDATE SYNCHRONISATION SET sync_num=?").use { stmt ->
            stmt.setInt(1, syncNum)
            stmt.execute()
        }
    }

}

fun getSyncNum(url: String) : Int? {

    var syncNum: Int? = null

    DriverManager.getConnection(url).use { conn ->
        conn.createStatement().use { stmt ->
            stmt.executeQuery("SELECT sync_num FROM SYNCHRONISATION WHERE id=0;").use { res ->
                res.next()
                syncNum = res.getInt(1)
            }
        }
    }

    return syncNum

}