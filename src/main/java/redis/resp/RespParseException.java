package redis.resp;

/**
 * Exception thrown when RESP parsing fails due to invalid format.
 * This is distinct from IOException which indicates connection problems.
 */
public class RespParseException extends Exception {
    public RespParseException(String message) {
        super(message);
    }
}
