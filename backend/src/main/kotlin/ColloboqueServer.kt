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
    private val databaseName by option("--pg-database", help="Name of the database").default("postgres")
    private val portNumber by option("--server-port", help="Number of the port").int().default(8080)
    private val postgresHost by option("--pg-host", help="host").default("localhost")
    private val postgresPort by option("--pg-port", help="Number of the port").int().default(5432)

    override fun run() {
        startServer(portNumber, postgresHost, postgresPort, databaseName)
    }

}


fun startServer(portNumber: Int, postgresHost: String, postgresPort: Int, databaseName: String) {
    val server = embeddedServer(Netty, port = portNumber) {
        routing {

            get("/") {
                call.respondText("Hello!")

            }

            post("/update") {
                val user = call.parameters["user"] ?: "postgres"
                val password = call.parameters["password"] ?: "123"

                val text = call.receiveText()
                val ds = connectPostgres(postgresHost, postgresPort, databaseName, user, password)

                UpdateDataBase(ds, text)

                call.respondText("Done")
            }

            get("/table") {

                val user = call.parameters["user"] ?: "postgres"
                val password = call.parameters["password"] ?: "123"
                val table = call.parameters["table"] ?: throw IllegalArgumentException("Table name expected")

                val ds = connectPostgres(postgresHost, postgresPort, databaseName, user, password)
                call.respondBytes(loadTableFromDB(ds, table).toByteArray())
            }

        }
    }
    server.start(wait = true)
}


fun main(args: Array<String>) = ColloboqueServer().main(args)