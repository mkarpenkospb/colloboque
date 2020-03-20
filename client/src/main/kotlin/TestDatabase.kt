import java.sql.DriverManager

fun main(args: Array<String>) {
    createFirst()
}

fun createFirst() {
    val url = "jdbc:h2:mem:"
    val conn = DriverManager.getConnection(url)
    val stmt = conn.createStatement()
    val sql = "CREATE TABLE   REGISTRATION " +
            "(id INTEGER not NULL, " +
            " first VARCHAR(255), " +
            " last VARCHAR(255), " +
            " age INTEGER, " +
            " PRIMARY KEY ( id ))"

    stmt.executeUpdate(sql)
}

