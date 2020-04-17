import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import io.ktor.client.HttpClient
import io.ktor.client.request.post
import kotlinx.coroutines.*


class Journal {
    private val currentQueries: MutableList<String> = ArrayList()
    private val history: MutableList<String> = ArrayList()

    fun addQuery(query: String) {
        currentQueries.add(query)
    }

    fun updateRequest(url: String, client: HttpClient) {

        runBlocking {
            client.post<String>(url) {
                body = jacksonObjectMapper().writeValueAsString(
                        UpdatePost(currentQueries)
                )
            }
        }

        history.addAll(currentQueries)
        currentQueries.clear()
    }
}

val clientJournal = Journal()



