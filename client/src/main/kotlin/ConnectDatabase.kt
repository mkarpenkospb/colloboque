import java.sql.DriverManager
import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import io.ktor.client.HttpClient
import io.ktor.client.request.post
import kotlinx.coroutines.runBlocking

fun importTable(url: String, tableName: String, tableData: ByteArray) {

    val tmp = createTempFile()
    tmp.writeBytes(tableData)

    DriverManager.getConnection(url).use { conn ->
        conn.createStatement().use { stmt ->
            val sql =
                    """
                     CREATE TABLE IF NOT EXISTS $tableName AS SELECT * FROM CSVREAD('$tmp');
                    """
            stmt.executeUpdate(sql)
        }
    }

    tmp.delete()
}

fun applyQueries(url: String, query: List<String>, clientLog: Log) {
    DriverManager.getConnection(url).use { conn ->
        conn.createStatement().use { stmt ->
            for (sql in query) {
                stmt.executeUpdate(sql)
                clientLog.writeLog(sql)
            }
        }
    }
}


data class UpdatePost(val statements: List<String>)

// expected queries as a kind of parameter
fun updateServer(urlServer: String, urlLocal: String, client: HttpClient): Int {

    val queries : MutableList<String> = ArrayList()
    var idToDelete = 0

    DriverManager.getConnection(urlLocal).use { conn ->
        conn.createStatement().use { stmt ->
            stmt.executeQuery("SELECT ID, SQL_COMMAND FROM LOG;").use { res ->
                while (res.next()) {
                    idToDelete = res.getString(1).toInt()
                    queries.add(res.getString(2))
                }
            }
        }
    }

    runBlocking {
        client.post<String>(urlServer) {
            body = jacksonObjectMapper().writeValueAsString(
                    UpdatePost(queries)
            )
        }
    }

    return idToDelete;

}