package exceptions;

public class InvalidParametersException extends RuntimeException {
    public InvalidParametersException(final String message) {
        super(message);
    }
}
