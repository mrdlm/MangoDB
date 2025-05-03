package commands;

public enum CommandType {
    PUT(2, "PUT expects a key and a value"),
    GET(1, "GET expects a key"),
    DELETE(1, "DELETE expects a key"),
    EXISTS(1, "EXISTS expects a key"),
    FLUSH(0, "FLUSH expects no parameters"),
    STATUS(0, "STATUS expects no parameters"),
    HELP(0, "HELP expects no parameters"),
    HEARTBEAT(2, "ERROR: Usage: HEARTBEAT <HOST> <PORT>"),
    REGISTER(3, "ERROR: Usage: REGISTER <PRIMARY|SECONDARY> <HOST> <PORT>"),
    SECONDARIES(0, "");

    private final int parameterCount;
    private final String errorMessage;

    CommandType(int parameterCount, String errorMessage) {
        this.parameterCount = parameterCount;
        this.errorMessage = errorMessage;
    }

    public int getParameterCount() {
        return parameterCount;
    }

    public String getErrorMessage() {
        return errorMessage;
    }
}
