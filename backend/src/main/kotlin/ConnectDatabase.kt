import com.github.ajalt.clikt.core.CliktCommand
import com.github.ajalt.clikt.parameters.options.default
import com.github.ajalt.clikt.parameters.options.option
import com.github.ajalt.clikt.parameters.types.int
import com.zaxxer.hikari.HikariDataSource



class ConnectDatabase : CliktCommand() {
    private val host by option("--pg-host", help="host").default("localhost")
    private val port by option("--pg-port", help="Number of the port").int().default(5432)
    private val dataBase by option("--pg-database", help="Name of the database").default("postgres")
    private val user by option("--pg-user", help="User name").default("postgres")
    private val password by option("--pg-password", help="User password").default("123")

    override fun run() {
        connectPostgres(host, port, dataBase, user, password);
    }
}


fun connectPostgres(host: String, port: Int, dataBase: String, user: String, password: String) {

    val url = "jdbc:postgresql://$host:$port/$dataBase"

    val ds = HikariDataSource()
    ds.jdbcUrl = url
    ds.username = user
    ds.password = password

}