import com.github.ajalt.clikt.core.CliktCommand
import com.github.ajalt.clikt.parameters.options.option
import com.github.ajalt.clikt.parameters.types.int
import io.ktor.application.*
import io.ktor.http.*
import io.ktor.response.*
import io.ktor.routing.*
import io.ktor.server.engine.*
import io.ktor.server.netty.*


class ColloboqueServer : CliktCommand() {
    private val portName by option("-p", help="Name of the port").int()

    override fun run() {
        portName?.let { startServer(it) }
    }
}


fun startServer(portName: Int) {
    val server = embeddedServer(Netty, port = portName) {
        routing {
            get("/") {
                call.respondText("Hello, word!", ContentType.Text.Html)
            }
            get("/echo") {
                val msg = call.parameters["msg"]
                call.respondText("$msg", ContentType.Text.Html)
            }
        }
    }
    server.start(wait = true)
}


fun main(args: Array<String>) = ColloboqueServer().main(args)