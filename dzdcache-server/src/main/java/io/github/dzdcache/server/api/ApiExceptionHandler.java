package io.github.dzdcache.server.api;

import io.github.dzdcache.cache.CacheException;
import org.springframework.http.ProblemDetail;
import org.springframework.web.bind.annotation.*;

@RestControllerAdvice
public class ApiExceptionHandler {
    @ExceptionHandler(CacheException.class)
    ProblemDetail cache(CacheException error) {
        ProblemDetail problem = ProblemDetail.forStatusAndDetail(org.springframework.http.HttpStatusCode.valueOf(error.status()), error.getMessage());
        if (error.reason() != null) problem.setProperty("reason", error.reason());
        return problem;
    }
    @ExceptionHandler({IllegalArgumentException.class, NullPointerException.class})
    ProblemDetail invalid(RuntimeException error) {
        return ProblemDetail.forStatusAndDetail(org.springframework.http.HttpStatus.BAD_REQUEST, error.getMessage() == null ? "Missing or invalid request field" : error.getMessage());
    }
}
