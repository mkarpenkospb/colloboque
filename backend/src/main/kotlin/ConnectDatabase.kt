import com.opencsv.CSVWriter
import com.zaxxer.hikari.HikariDataSource
import java.io.BufferedWriter
import java.io.ByteArrayOutputStream
import java.io.OutputStreamWriter
import java.nio.file.Files
import java.nio.file.Paths
import java.sql.ResultSetMetaData


fun connectPostgres(host: String?, port: Int?,
                    dataBase: String?, user: String?, password: String?): HikariDataSource {

    val url = "jdbc:postgresql://$host:$port/$dataBase"

    val ds = HikariDataSource()
    ds.jdbcUrl = url
    ds.username = user
    ds.password = password

    return ds
}


fun loadTableFromDB(ds : HikariDataSource, tableName : String? ): ByteArrayOutputStream {

    val query =
            """
                SELECT * FROM $tableName
            """
    val queryByteArray = ByteArrayOutputStream()

    ds.connection.use { conn ->
        conn.createStatement().use { stmt ->
            stmt.executeQuery(query).use {rs ->
                val rsmd: ResultSetMetaData = rs.metaData

                val colNames = arrayOfNulls<String>(rsmd.columnCount)

                for (i in 1..rsmd.columnCount) {
                    colNames[i-1] = rsmd.getColumnName(i)
                }

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
            }
        }
    }
    return queryByteArray
}

