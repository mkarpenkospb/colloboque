import java.sql.DriverManager


/**
 * example of some simple queries
 */
fun createFirst() {
    val url = "jdbc:h2:~/bd1"
    DriverManager.getConnection(url).use { conn ->
        conn.createStatement().use { stmt ->
            stmt.executeQuery("SELECT * from newtable").use { res ->
                while (res.next()) {
                    println("${res.getString(1)}  ${res.getString(2)}  ${res.getString(3)}  ${res.getString(4)}")
                }
            }
        }
    }
}


fun importTable(url: String, tableName: String?, fromFile: String) {
    DriverManager.getConnection(url).use { conn ->
        conn.createStatement().use { stmt ->
            val sql =
                    """
                     CREATE TABLE $tableName AS SELECT * FROM CSVREAD('$fromFile');
                    """
            stmt.executeUpdate(sql)
        }
    }
}

