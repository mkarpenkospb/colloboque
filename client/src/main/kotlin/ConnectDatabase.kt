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

fun applyQueries(url: String, query: List<String>) {
    DriverManager.getConnection(url).use { conn ->
        conn.createStatement().use { stmt ->
            for (sql in query) {
                stmt.executeUpdate(sql)
                clientLog.addQuery(sql)
            }
        }
    }
}


data class UpdatePost(val statements: List<String>)


fun connectServer(url: String, client: HttpClient, queries: MutableList<String>) {
    runBlocking {
        client.post<String>(url) {
            body = jacksonObjectMapper().writeValueAsString(
                    UpdatePost(queries)
            )
        }
    }
}

