package com.uw.hub.infrastructure.logging;

import jakarta.servlet.*;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import lombok.extern.slf4j.Slf4j;
import org.slf4j.MDC;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.util.UUID;

/**
 * Filter to add correlation IDs to all requests for distributed tracing.
 * Correlation IDs are stored in MDC for logging and passed to downstream services.
 */
@Component
@Slf4j
public class CorrelationIdFilter implements Filter {

    private static final String CORRELATION_ID_HEADER = "X-Correlation-ID";
    private static final String CORRELATION_ID_MDC_KEY = "correlationId";

    @Override
    public void doFilter(ServletRequest request, ServletResponse response, FilterChain chain)
        throws IOException, ServletException {

        HttpServletRequest httpRequest = (HttpServletRequest) request;
        HttpServletResponse httpResponse = (HttpServletResponse) response;

        try {
            // Get or generate correlation ID
            String correlationId = httpRequest.getHeader(CORRELATION_ID_HEADER);
            if (correlationId == null || correlationId.isEmpty()) {
                correlationId = generateCorrelationId();
            }

            // Store in MDC for logging
            MDC.put(CORRELATION_ID_MDC_KEY, correlationId);

            // Add to response headers
            httpResponse.setHeader(CORRELATION_ID_HEADER, correlationId);

            // Only log API requests, skip static resources and common browser requests
            String requestUri = httpRequest.getRequestURI();
            if (shouldLogRequest(requestUri)) {
                log.debug("Processing request {} with correlation ID: {}", requestUri, correlationId);
            }

            // Continue filter chain
            chain.doFilter(request, response);

        } finally {
            // Always clear MDC to prevent memory leaks
            MDC.remove(CORRELATION_ID_MDC_KEY);
        }
    }

    /**
     * Determine if a request should be logged to reduce noise
     */
    private boolean shouldLogRequest(String requestUri) {
        // Skip logging for common static resources and health checks
        return requestUri != null
            && !requestUri.endsWith(".ico")
            && !requestUri.endsWith(".png")
            && !requestUri.endsWith(".jpg")
            && !requestUri.endsWith(".svg")
            && !requestUri.endsWith(".css")
            && !requestUri.endsWith(".js")
            && !requestUri.endsWith(".map")
            && !requestUri.startsWith("/actuator/health");
    }

    /**
     * Generate a new correlation ID
     */
    private String generateCorrelationId() {
        return UUID.randomUUID().toString();
    }

    /**
     * Get current correlation ID from MDC
     */
    public static String getCurrentCorrelationId() {
        return MDC.get(CORRELATION_ID_MDC_KEY);
    }
}
