import java.sql.DriverManager
import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import kotlinx.coroutines.runBlocking
import com.fasterxml.jackson.module.kotlin.readValue
import com.opencsv.CSVWriter
import io.ktor.client.request.post
import org.apache.commons.codec.binary.Base64
import java.io.BufferedWriter
import java.io.ByteArrayOutputStream
import java.io.OutputStreamWriter
import java.sql.Connection
import java.sql.ResultSetMetaData


data class ReplicationResponse(val csvbase64: ByteArray, val sync_num: Int)
data class UpdateRequest(val statements: List<String>, val sync_num: Int, val user_id: String)
data class MergeRequest(val statements: List<String>, val csvbase64: ByteArray,
                        val sync_num: Int, val user_id: String)


fun importTable(client: Client, tableName: String, tableData: ByteArray) {

    val update: ReplicationResponse = jacksonObjectMapper().readValue(tableData)
    val tmp = createTempFile()
    tmp.writeText(String(Base64.decodeBase64(update.csvbase64)))
    client.txnManager.transaction { conn ->
        conn.createStatement().use { stmt ->
            stmt.execute("DROP TABLE IF EXISTS $tableName")
            val sql =
                    """
                     CREATE TABLE $tableName AS SELECT * FROM CSVREAD('${tmp.absolutePath}');
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
            body = jacksonObjectMapper().writeValueAsString(
                    UpdateRequest(queries, getSyncNum(client.connectionUrl), client.userId)
            )
        }.toInt()
    }

    // response -2 means client is behind
    if (response == -2) {
        response = runBlocking {
            client.httpClient.post<String>("http://$serverHost:$serverPort/merge") {
                body = loadTableFromDB(client.connectionUrl, queries, client.userId)
            }.toInt()
        }
        loadTableFromServer(client, serverHost, serverPort, "TABLE2")
    }

    updateSyncNum(DriverManager.getConnection(client.connectionUrl), response)

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
    val rs = conn.metaData.getTables(null, null, tableName, null)
    while (rs.next()) {
        return true
    }
    return false
}

class TransactionManager(val connectionUrl: String) {
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

// copy pasted function from server but forms another data class
// temporary table name inlined
fun loadTableFromDB(connectionUrl: String, queries: List<String>, userId: String,
                    tableName: String = "TABLE2_CLONE"): ByteArray {
    val syncNum = getSyncNum(connectionUrl) // better overload?
    DriverManager.getConnection(connectionUrl).use { conn ->
        conn.autoCommit = false
        conn.createStatement().use { stmt ->
            val query =
                    """
                    SELECT * FROM $tableName
                    """
            stmt.executeQuery(query).use { rs ->
                val rsmd: ResultSetMetaData = rs.metaData

                val colNames = arrayOfNulls<String>(rsmd.columnCount)

                for (i in 1..rsmd.columnCount) {
                    colNames[i - 1] = rsmd.getColumnName(i)
                }
                val queryByteArray = ByteArrayOutputStream()

                CSVWriter(BufferedWriter(OutputStreamWriter(queryByteArray)),
                        CSVWriter.DEFAULT_SEPARATOR,
                        CSVWriter.NO_QUOTE_CHARACTER,
                        CSVWriter.DEFAULT_ESCAPE_CHARACTER,
                        CSVWriter.DEFAULT_LINE_END).use { csvWriter ->

                    csvWriter.writeNext(colNames)
                    while (rs.next()) {
                        val getLine = arrayOfNulls<String>(rsmd.columnCount)
                        for (i in 1..rsmd.columnCount) {
                            getLine[i - 1] = rs.getString(i)
                        }
                        csvWriter.writeNext(getLine)
                    }
                }
                conn.commit()
                conn.autoCommit = true
                return jacksonObjectMapper().writeValueAsBytes(MergeRequest(queries,
                        Base64.encodeBase64(queryByteArray.toByteArray()), syncNum, userId))
            }
        }
    }
}

