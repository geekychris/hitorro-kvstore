package com.hitorro.kvstore;

import java.util.Optional;
import java.util.function.Consumer;
import java.util.function.Function;

/**
 * Result type for operations that may succeed or fail without throwing exceptions.
 * Provides a functional approach to error handling.
 *
 * @param <T> The type of the success value
 */
public class Result<T> {
    private final T value;
    private final String error;
    private final boolean success;

    private Result(T value, String error, boolean success) {
        this.value = value;
        this.error = error;
        this.success = success;
    }

    /**
     * Creates a successful result with the given value.
     *
     * @param value The success value
     * @param <T> The type of the value
     * @return A successful Result
     */
    public static <T> Result<T> success(T value) {
        return new Result<>(value, null, true);
    }

    /**
     * Creates a failed result with the given error message.
     *
     * @param error The error message
     * @param <T> The type parameter
     * @return A failed Result
     */
    public static <T> Result<T> failure(String error) {
        return new Result<>(null, error, false);
    }

    /**
     * Creates a failed result from an exception.
     *
     * @param exception The exception
     * @param <T> The type parameter
     * @return A failed Result
     */
    public static <T> Result<T> failure(Exception exception) {
        return new Result<>(null, exception.getMessage(), false);
    }

    /**
     * Checks if this result represents success.
     *
     * @return true if successful, false otherwise
     */
    public boolean isSuccess() {
        return success;
    }

    /**
     * Checks if this result represents failure.
     *
     * @return true if failed, false otherwise
     */
    public boolean isFailure() {
        return !success;
    }

    /**
     * Gets the success value as an Optional.
     *
     * @return Optional containing the value if successful, empty otherwise
     */
    public Optional<T> getValue() {
        return Optional.ofNullable(value);
    }

    /**
     * Gets the error message as an Optional.
     *
     * @return Optional containing the error if failed, empty otherwise
     */
    public Optional<String> getError() {
        return Optional.ofNullable(error);
    }

    /**
     * Gets the value or throws an exception if failed.
     *
     * @return The value
     * @throws IllegalStateException if the result is a failure
     */
    public T getOrThrow() {
        if (!success) {
            throw new IllegalStateException("Result is a failure: " + error);
        }
        return value;
    }

    /**
     * Gets the value or returns a default value if failed.
     *
     * @param defaultValue The default value
     * @return The value if successful, default value otherwise
     */
    public T getOrDefault(T defaultValue) {
        return success ? value : defaultValue;
    }

    /**
     * Maps the success value to a new type.
     *
     * @param mapper The mapping function
     * @param <U> The new type
     * @return A new Result with the mapped value
     */
    public <U> Result<U> map(Function<T, U> mapper) {
        if (success) {
            try {
                return Result.success(mapper.apply(value));
            } catch (Exception e) {
                return Result.failure(e);
            }
        }
        return Result.failure(error);
    }

    /**
     * Flat maps the success value to a new Result.
     *
     * @param mapper The mapping function
     * @param <U> The new type
     * @return The new Result
     */
    public <U> Result<U> flatMap(Function<T, Result<U>> mapper) {
        if (success) {
            try {
                return mapper.apply(value);
            } catch (Exception e) {
                return Result.failure(e);
            }
        }
        return Result.failure(error);
    }

    /**
     * Executes a consumer if the result is successful.
     *
     * @param consumer The consumer to execute
     * @return This result for chaining
     */
    public Result<T> ifSuccess(Consumer<T> consumer) {
        if (success) {
            consumer.accept(value);
        }
        return this;
    }

    /**
     * Executes a consumer if the result is a failure.
     *
     * @param consumer The consumer to execute
     * @return This result for chaining
     */
    public Result<T> ifFailure(Consumer<String> consumer) {
        if (!success) {
            consumer.accept(error);
        }
        return this;
    }

    @Override
    public String toString() {
        if (success) {
            return "Result.Success[" + value + "]";
        } else {
            return "Result.Failure[" + error + "]";
        }
    }
}
