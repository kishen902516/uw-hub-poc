package com.uw.hub.infrastructure.config;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Configuration;
import org.springframework.web.servlet.config.annotation.AsyncSupportConfigurer;
import org.springframework.web.servlet.config.annotation.CorsRegistry;
import org.springframework.web.servlet.config.annotation.WebMvcConfigurer;

/**
 * Web MVC configuration for CORS and other web settings.
 * Enables cross-origin requests from the frontend application.
 */
@Configuration
public class WebMvcConfig implements WebMvcConfigurer {

    @Value("${cors.allowed-origins:http://localhost:3000,http://localhost:3001}")
    private String allowedOriginsStr;

    @Value("${sse.timeout:3600000}")
    private long sseTimeout;

    @Override
    public void configureAsyncSupport(AsyncSupportConfigurer configurer) {
        // Configure async request timeout for SSE (should match or exceed SSE emitter timeout)
        configurer.setDefaultTimeout(sseTimeout);
    }

    @Override
    public void addCorsMappings(CorsRegistry registry) {
        String[] allowedOrigins = allowedOriginsStr.split(",");

        registry.addMapping("/api/**")
            .allowedOrigins(allowedOrigins)
            .allowedMethods("GET", "POST", "PUT", "DELETE", "OPTIONS")
            .allowedHeaders("*")
            .exposedHeaders("*")
            .allowCredentials(true)
            .maxAge(3600);

        // SSE endpoint requires specific CORS configuration
        registry.addMapping("/api/sse/**")
            .allowedOrigins(allowedOrigins)
            .allowedMethods("GET", "OPTIONS")
            .allowedHeaders("*")
            .exposedHeaders("*")
            .allowCredentials(true)
            .maxAge(3600);
    }
}
