package exceptions;

public class ReservedKeywordException extends RuntimeException {
    public ReservedKeywordException(String message){
        super(message);
    }
}
