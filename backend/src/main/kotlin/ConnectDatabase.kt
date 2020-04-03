import com.zaxxer.hikari.HikariDataSource
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


fun loadTableFromDB(ds : HikariDataSource, tableName : String? ): String {

    val query =
            """
                SELECT * FROM $tableName
            """

    val buff = StringBuffer()
    val colNames: ArrayList<String> = ArrayList()

    ds.connection.use { conn ->
        conn.createStatement().use { stmt ->
            stmt.executeQuery(query).use {rs ->
                val rsmd: ResultSetMetaData = rs.metaData
                for (i in 1..rsmd.columnCount) {
                    val colName = rsmd.getColumnName(i);
                    buff.append(colName)
                    if (i < rsmd.columnCount)
                        buff.append(',')
                    colNames.add(colName)
                }
                buff.append('\n')
                while (rs.next()) {
                    for (i in 1..rsmd.columnCount) {
                        buff.append(rs.getString(i))
                        if (i < rsmd.columnCount)
                            buff.append(',')
                    }
                    buff.append('\n')
                }
            }
        }
    }

    return buff.toString()
}