package dev.jobscheduler.api;

import org.springframework.http.HttpStatus;
import org.springframework.http.ProblemDetail;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;

import dev.jobscheduler.domain.SchedulerException;

@RestControllerAdvice
public class ApiExceptionHandler {

    @ExceptionHandler(SchedulerException.NotFound.class)
    ProblemDetail notFound(SchedulerException e) {
        return ProblemDetail.forStatusAndDetail(HttpStatus.NOT_FOUND, e.getMessage());
    }

    @ExceptionHandler(SchedulerException.Conflict.class)
    ProblemDetail conflict(SchedulerException e) {
        return ProblemDetail.forStatusAndDetail(HttpStatus.CONFLICT, e.getMessage());
    }

    @ExceptionHandler(SchedulerException.InvalidRequest.class)
    ProblemDetail invalid(SchedulerException e) {
        return ProblemDetail.forStatusAndDetail(HttpStatus.BAD_REQUEST, e.getMessage());
    }
}
