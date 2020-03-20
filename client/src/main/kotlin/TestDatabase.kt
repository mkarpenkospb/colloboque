import java.sql.DriverManager

/**
 * example of some simple queries
 */
fun createFirst() {
    val url = "jdbc:h2:~/bd1"
    DriverManager.getConnection(url).use { conn ->
        conn.createStatement().use { stmt ->
            val sql =
             """
             CREATE TABLE   REGISTRATION
            (id INTEGER not NULL,
             first VARCHAR(255),
             last VARCHAR(255),
             age INTEGER,
             PRIMARY KEY ( id ))
            """
            stmt.executeUpdate(sql)
            stmt.executeUpdate("""
            INSERT INTO REGISTRATION(id, first, last, age)
            VALUES(2, 'Adam', 'Smith', 44 )
            """)
            stmt.executeQuery("SELECT * from REGISTRATION").use { res ->
                while (res.next()) {
                    println("${res.getString(1)}  ${res.getString(2)}  ${res.getString(3)}  ${res.getString(4)}")
                }
            }
        }
    }
}

