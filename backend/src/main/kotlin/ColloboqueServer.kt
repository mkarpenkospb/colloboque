import com.github.ajalt.clikt.core.CliktCommand
import com.github.ajalt.clikt.parameters.options.default
import com.github.ajalt.clikt.parameters.options.option
import com.github.ajalt.clikt.parameters.types.int
import io.ktor.application.*
import io.ktor.response.*
import io.ktor.routing.*
import io.ktor.server.engine.*
import io.ktor.server.netty.*
import io.ktor.request.*


class ColloboqueServer : CliktCommand() {
    private val databaseName by option("--pg-database", help = "Name of the database").default("postgres")
    private val portNumber by option("--server-port", help = "Number of the port").int().default(8080)
    private val postgresHost by option("--pg-host", help = "host").default("localhost")
    private val postgresPort by option("--pg-port", help = "Number of the port").int().default(5432)

    override fun run() {
        startServer(portNumber, postgresHost, postgresPort, databaseName)
    }

}


fun startServer(portNumber: Int, postgresHost: String, postgresPort: Int, databaseName: String) {

    val DEFAULT_USER = "tester"
    val DEFAULT_PASSWORD = "test_password"

    val server = embeddedServer(Netty, port = portNumber) {
        routing {

            get("/") {
                call.respondText("Hello!")

            }

            post("/update") {

                val text = call.receiveText()
                val ds = connectPostgres(postgresHost, postgresPort, databaseName,
                        DEFAULT_USER, DEFAULT_PASSWORD)

                updateDataBase(ds, text)

                call.respondText("Done")
            }

            get("/table") {

                val table = call.parameters["table"] ?: "baseTable"

                val ds = connectPostgres(postgresHost, postgresPort, databaseName,
                        DEFAULT_PASSWORD, DEFAULT_PASSWORD)
                call.respondBytes(loadTableFromDB(ds, table))
            }
        }
    }
    server.start(wait = true)
}


fun main(args: Array<String>) = ColloboqueServer().main(args)