import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
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
import io.ktor.util.toByteArray
import org.apache.commons.codec.binary.Base64


class ColloboqueServer : CliktCommand() {
    private val databaseName by option("--pg-database", help = "Name of the database").default("postgres")
    private val portNumber by option("--server-port", help = "Number of the port").int().default(8080)
    private val postgresHost by option("--pg-host", help = "host").default("localhost")
    private val postgresPort by option("--pg-port", help = "Number of the port").int().default(5432)
    private val user by option("--pg-user", help = "Default user name").default("postgres")
    private val password by option("--pg-password", help = "Default password").default("")
    private val table by option("--table", help = "Default table name").default("MAIN_TABLE")


    override fun run() {
        startServer(portNumber, postgresHost, postgresPort, databaseName, user, password, table)
    }
}


fun startServer(portNumber: Int, postgresHost: String, postgresPort: Int,
                databaseName: String, user: String, password: String, table: String) {

    val ds = connectPostgres(postgresHost, postgresPort, databaseName, user, password)
    setUpServer(ds)
    val serverLog = ServerLog(ds)
    val server = embeddedServer(Netty, port = portNumber) {
        routing {

            get("/") {
                call.respondText("Hello!")
            }

            post("/update") {
                val syncNum = updateDataBase(ds, call.receiveText(), serverLog)

                if (syncNum == -1) {
                    throw RuntimeException("Server update operation failed")
                }

                //  in case syncNum == -2 client has an action
                call.respondText(syncNum.toString())
            }

            post("/merge") {

                mergeDataBase(ds, call.receiveChannel().toByteArray())

                call.respondText(getSyncNum(ds.connection).toString())
            }

            get("/table") {
                // bad for SyncNum and table date, SyncNum might change
                call.respondBytes(jacksonObjectMapper().writeValueAsBytes(ReplicationResponse(
                        Base64.encodeBase64(loadTableFromDB(ds.connection,  table)),
                        getSyncNum(ds.connection)
                )))
            }
        }
    }
    server.start(wait = true)
}


fun main(args: Array<String>) = ColloboqueServer().main(args)