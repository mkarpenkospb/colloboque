import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import com.fasterxml.jackson.module.kotlin.readValue
import com.opencsv.CSVWriter
import com.zaxxer.hikari.HikariDataSource
import org.apache.commons.codec.binary.Base64
import org.postgresql.copy.CopyManager
import org.postgresql.core.BaseConnection
import java.io.*
import java.sql.Connection
import java.sql.Connection.TRANSACTION_SERIALIZABLE
import java.sql.ResultSetMetaData


private const val CALL_FUNCTION = "{call clone_schema(?, ?, true)}"
private const val BASE_SCHEMA_NAME = "schema"

data class ReplicationResponse(val csvbase64: ByteArray, val sync_num: Int)
data class UpdateRequest(val statements: List<String>, val sync_num: Int, val user_id: String)
data class MergeRequest(val statements: List<String>, val csvbase64: ByteArray,
                        val sync_num: Int, val user_id: String)



fun connectPostgres(host: String, port: Int, dataBase: String, currentSchema: String,
                    user: String, password: String): HikariDataSource {

    val ds = HikariDataSource()
    ds.jdbcUrl = "jdbc:postgresql://$host:$port/$dataBase?currentSchema=$currentSchema"
    ds.username = user
    ds.password = password
    ds.schema = currentSchema
    return ds
}


fun updateDataBase(ds: HikariDataSource, jsonQueries: String, serverLog: Log): Int {
    val update: UpdateRequest = jacksonObjectMapper().readValue(jsonQueries)
    return ds.connection.use { conn ->
        conn.autoCommit = false
        val serverSyncNum = getSyncNum(conn)
        if (serverSyncNum == update.sync_num) {
            runUpdateRequest(conn, update)
            serverLog.writeLog(conn, update)
            conn.commit()
            conn.autoCommit = true
            update.sync_num + 1
        } else if (serverSyncNum > update.sync_num) {
            -2
        } else {
            -1
        }
    }
}


// current temporary table name
fun mergeDataBase(ds: HikariDataSource, postgresHost: String, postgresPort: Int,
                  databaseName: String, currentSchema: String, user: String,
                  password: String, oldClientTable: ByteArray, tableName: String = "MAIN_TABLE") : HikariDataSource {

    val mergeData: MergeRequest = jacksonObjectMapper().readValue(oldClientTable)
    val queries = mergeData.statements.toMutableList()
    val currentSchemaVersion = currentSchema.substring(BASE_SCHEMA_NAME.length).toInt()
    val newSchema = "${BASE_SCHEMA_NAME}${currentSchemaVersion + 1}"

    // create and call function
    transaction(ds, ds.schema) { conn ->
        conn.createStatement().use {stmt->
            stmt.execute(CLONE_SCHEMA)
        }
        conn.prepareCall(CALL_FUNCTION).use { cstmt ->
            cstmt.setString(1, currentSchema)
            cstmt.setString(2, newSchema)
            cstmt.execute()
        }
    }

    // the same as in client part
    val tmp = createTempFile()
    tmp.writeText(String(Base64.decodeBase64(mergeData.csvbase64)))
    transaction(ds, ds.schema) {conn ->
        conn.createStatement().use{stmt->
            stmt.execute("DROP TABLE IF EXISTS ${newSchema}.${tableName}")
            stmt.execute("""CREATE TABLE IF NOT EXISTS ${newSchema}.${tableName} (
                            id INT PRIMARY KEY NOT NULL,
                            first TEXT NOT NULL,
                            last TEXT NOT NULL,
                            age INT NOT NULL);
                            """)
        }
        CopyManager(conn.unwrap(BaseConnection::class.java))
                .copyIn(
                        "COPY ${newSchema}.${tableName} FROM STDIN (FORMAT csv, HEADER)",
                        Base64.decodeBase64(mergeData.csvbase64).inputStream()
                )
    }
    tmp.delete()

    val dsNew = connectPostgres(postgresHost, postgresPort, databaseName, newSchema, user, password)
    // Select all the newest from log
    transaction(dsNew, dsNew.schema) {conn->
        conn.transactionIsolation = TRANSACTION_SERIALIZABLE
        conn.createStatement().use { stmt->
            stmt.executeQuery(
                    """SELECT sql_command FROM LOG 
                       WHERE sync_num > ${mergeData.sync_num}
                       ORDER BY sync_num, q_id""").use {res ->
                while (res.next()) {
                    queries.add(res.getString(1))
                }
            }
        }
    }

    try {
        transaction(dsNew, dsNew.schema) { conn ->
            conn.transactionIsolation = TRANSACTION_SERIALIZABLE
            conn.createStatement().use {stmt ->
                for (sql in queries) {
                    stmt.execute(sql)
                }
                updateSyncNum(conn, getSyncNum(conn) + 1)
            }
        }
        return dsNew
    } catch (e : Throwable) {
        TODO("action in case merging failed")
    }
}

fun runUpdateRequest(conn: Connection, update: UpdateRequest) {
    conn.createStatement().use { stmt ->
        for (q in update.statements) {
            stmt.execute(q)
        }
    }
    updateSyncNum(conn, update.sync_num + 1)
}


fun updateSyncNum(conn: Connection, syncNum: Int) {
    conn.prepareStatement("UPDATE SYNCHRONISATION SET sync_num=?").use { stmt ->
        stmt.setInt(1, syncNum)
        stmt.execute()
    }
}


fun getSyncNum(conn: Connection) : Int {
    conn.createStatement().use { stmt ->
        stmt.executeQuery("SELECT sync_num FROM SYNCHRONISATION WHERE id=0;").use { res ->
            res.next()
            return res.getInt(1)
        }
    }
}


fun <T> transaction(ds: HikariDataSource,schema: String, code: (Connection) -> T): T {
    return ds.connection.use { conn ->
        conn.autoCommit = false
        code(conn).also {
            conn.prepareStatement("SET search_path = ?").use {stmt ->
                stmt.setString(1, schema)
                stmt.execute()
            }
            conn.commit()
            conn.autoCommit = true
        }
    }
}


fun loadTableFromDB(ds: HikariDataSource, tableName: String): ByteArray {
    ds.connection.use { conn ->
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
                val syncNum = getSyncNum(conn)
                conn.commit()
                conn.autoCommit = true
                return jacksonObjectMapper().writeValueAsBytes(ReplicationResponse(
                        Base64.encodeBase64(queryByteArray.toByteArray()), syncNum))
            }
        }
    }
}

