import com.opencsv.CSVWriter
import com.zaxxer.hikari.HikariDataSource
import java.io.BufferedWriter
import java.io.ByteArrayOutputStream
import java.io.OutputStreamWriter
import java.sql.ResultSetMetaData
import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import com.fasterxml.jackson.module.kotlin.readValue
import org.apache.commons.codec.binary.Base64
import java.sql.Connection


data class ReplicationResponse(val csvbase64: ByteArray, val sync_num: Int)
data class UpdateRequest(val statements: List<String>, val sync_num: Int)


fun connectPostgres(host: String, port: Int,
                    dataBase: String, user: String, password: String): HikariDataSource {

    val ds = HikariDataSource()
    ds.jdbcUrl = "jdbc:postgresql://$host:$port/$dataBase"
    ds.username = user
    ds.password = password

    return ds
}


fun updateDataBase(ds: HikariDataSource, jsonQueries: String): Int {
    val update: UpdateRequest = jacksonObjectMapper().readValue(jsonQueries)
    ds.connection.use { conn ->
        conn.autoCommit = false
        conn.createStatement().use { stmt ->
            for (q in update.statements) {
                stmt.execute(q)
            }
        }
        updateSyncNum(conn, update.sync_num + 1)
        conn.commit()
        conn.autoCommit = true
    }

    return update.sync_num + 1;
}


fun updateSyncNum(conn: Connection, syncNum: Int) {
    conn.prepareStatement("UPDATE SYNCHRONISATION SET sync_num=?").use { stmt ->
        stmt.setInt(1, syncNum)
        stmt.execute()
    }
}


fun getSyncNum(ds: HikariDataSource) : Int {
    ds.connection.use { conn ->
        conn.createStatement().use { stmt ->
            stmt.executeQuery("SELECT sync_num FROM SYNCHRONISATION WHERE id=0;").use { res ->
                res.next()
                return res.getInt(1)
            }
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
                val syncNum = getSyncNum(ds)
                conn.commit()
                conn.autoCommit = true
                return jacksonObjectMapper().writeValueAsBytes(ReplicationResponse(
                        Base64.encodeBase64(queryByteArray.toByteArray()), syncNum))
            }
        }
    }
}

