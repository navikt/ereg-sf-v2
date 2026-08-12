package no.nav.template

import io.prometheus.client.CollectorRegistry
import io.prometheus.client.Counter
import io.prometheus.client.Gauge
import io.prometheus.client.Histogram
import io.prometheus.client.Summary
import io.prometheus.client.exporter.common.TextFormat
import io.prometheus.client.hotspot.DefaultExports
import mu.KotlinLogging
import org.http4k.core.HttpHandler
import org.http4k.core.Response
import org.http4k.core.Status
import java.io.StringWriter

object Metrics {
    private val log = KotlinLogging.logger { }

    val cRegistry: CollectorRegistry = CollectorRegistry.defaultRegistry

    val apiCalls: Counter = registerLabelCounter("api_calls", "ingress")

    fun registerForwardedCallHistogram(name: String): Histogram =
        Histogram
            .build()
            .name(name)
            .help(name)
            .labelNames("targetApp", "tokenType", "status")
            .buckets(50.0, 100.0, 200.0, 300.0, 400.0, 500.0, 1000.0, 2000.0, 4000.0)
            .register()

    fun registerSummary(name: String) =
        Summary
            .build()
            .name(name)
            .help(name)
            .register()

    fun registerGauge(name: String) =
        Gauge
            .build()
            .name(name)
            .help(name)
            .register()

    fun registerLabelGauge(
        name: String,
        vararg labels: String,
    ) = Gauge
        .build()
        .name(name)
        .help(name)
        .labelNames(*labels)
        .register()

    fun registerLabelCounter(
        name: String,
        vararg labels: String,
    ) = Counter
        .build()
        .name(name)
        .help(name)
        .labelNames(*labels)
        .register()

    init {
        DefaultExports.initialize()
    }

    val metricsHttpHandler: HttpHandler = {
        try {
            val str = StringWriter()
            TextFormat.write004(str, CollectorRegistry.defaultRegistry.metricFamilySamples())
            val result = str.toString()
            if (result.isEmpty()) {
                Response(Status.NO_CONTENT)
            } else {
                Response(Status.OK).body(result)
            }
        } catch (e: Exception) {
            log.error { "/prometheus failed writing metrics - ${e.message}" }
            Response(Status.INTERNAL_SERVER_ERROR)
        }
    }
}
