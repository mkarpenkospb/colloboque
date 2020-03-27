import io.ktor.application.*
import io.ktor.http.*
import io.ktor.response.*
import io.ktor.routing.*
import io.ktor.server.engine.*
import io.ktor.server.netty.*
import com.github.ajalt.clikt.core.CliktCommand
import com.github.ajalt.clikt.parameters.options.option
import com.github.ajalt.clikt.parameters.types.int


class ColloboqueServer : CliktCommand() {
    private val portName by option("-p", help="Name of the port").int()

    override fun run() {
        portName?.let { startServer(it) }
    }
}


fun startServer(potrName: Int) {
    val server = embeddedServer(Netty, port = potrName) {
        routing {
            get("/") {
                call.respondText("Hello, word!", ContentType.Text.Html)
            }
            get("/echo") {
                val msg: String? = call.parameters["msg"]
                call.respondText("$msg", ContentType.Text.Html)
            }
        }
    }
    server.start(wait = true)
}

fun main(args: Array<String>) = ColloboqueServer().main(args)