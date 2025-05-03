package commands;

import exceptions.InvalidCommandException;
import exceptions.InvalidParametersException;
import exceptions.ReservedKeywordException;

import java.util.Objects;

import static commands.CommandType.DELETE;
import static commands.CommandType.EXISTS;
import static commands.CommandType.FLUSH;
import static commands.CommandType.GET;
import static commands.CommandType.HEARTBEAT;
import static commands.CommandType.HELP;
import static commands.CommandType.PUT;
import static commands.CommandType.REGISTER;
import static commands.CommandType.STATUS;
import static legacy.engine.LogWriter.FLUSH_TOMBSTONE_VALUE;
import static legacy.engine.LogWriter.TOMBSTONE_VALUE;

public class CommandParser {

    private static final String SPACE_DELIMITER = " ";

    public static Command parse(final String input) {

        if (Objects.isNull(input) || input.isEmpty()) {
            return null;
        }

        int firstSpaceIndex = input.indexOf(SPACE_DELIMITER);
        CommandType commandType;
        String argsString = "";

        try {
            if (firstSpaceIndex == -1) {
                commandType = CommandType.valueOf(input.toUpperCase());
            } else {
                commandType = CommandType.valueOf(input.substring(0, firstSpaceIndex).toUpperCase());
                argsString = input.substring(firstSpaceIndex + 1).strip();
            }
        } catch (final IllegalArgumentException e) {
            throw new InvalidCommandException(e.getMessage());
        }

        return switch (commandType) {
            case PUT -> handlePut(argsString);
            case GET -> handleGet(argsString);
            case DELETE -> handleDelete(argsString);
            case FLUSH -> handleFlush(argsString);
            case EXISTS -> handleExists(argsString);
            case STATUS -> handleStatus(argsString);
            case HELP -> handleHelp(argsString);
            case HEARTBEAT -> handleHeartbeat(argsString);
            case REGISTER -> handleRegister(argsString);
        };
    }

    private static Command handleRegister(final String argsString) {
        String[] args = argsString.strip().split(SPACE_DELIMITER);

        if (args.length < REGISTER.getParameterCount() || args.length > REGISTER.getParameterCount()) {
            throw new InvalidParametersException(REGISTER.getErrorMessage());
        }

        return new Command(REGISTER, args);
    }

    private static Command handleHeartbeat(final String argsString) {
        return new Command(HEARTBEAT, null);
    }

    private static Command handleHelp(final String argsString) {
        if (!argsString.isEmpty()) {
            throw new InvalidParametersException(HELP.getErrorMessage());
        }

        return new Command(HELP, null);
    }

    private static Command handleStatus(final String argsString) {
        if (!argsString.isEmpty()) {
            throw new InvalidParametersException(STATUS.getErrorMessage());
        }

        return new Command(STATUS, null);
    }

    private static Command handleFlush(final String argsString) {
        if (!argsString.isEmpty()) {
            throw new InvalidParametersException(FLUSH.getErrorMessage());
        }

        return new Command(FLUSH, null);
    }

    private static Command handleDelete(final String argsString) {
        if (argsString.isEmpty()) {
            throw new InvalidParametersException(DELETE.getErrorMessage());
        }

        String[] args = argsString.strip().split(SPACE_DELIMITER);

        if (args.length != DELETE.getParameterCount()) {
            throw new InvalidParametersException(DELETE.getErrorMessage());
        }

        return new Command(DELETE, args);
    }


    private static Command handleExists(final String argsString) {
        if (argsString.isEmpty()) {
            throw new InvalidParametersException(EXISTS.getErrorMessage());
        }

        String[] args = argsString.strip().split(SPACE_DELIMITER);

        if (args.length != EXISTS.getParameterCount()) {
            throw new InvalidParametersException(EXISTS.getErrorMessage());
        }

        return new Command(EXISTS, args);
    }

    private static Command handleGet(final String argsString) {
        if (argsString.isEmpty()) {
            throw new InvalidParametersException(GET.getErrorMessage());
        }

        String[] args = argsString.strip().split(SPACE_DELIMITER);

        if (args.length != GET.getParameterCount()) {
            throw new InvalidParametersException(GET.getErrorMessage());
        }

        return new Command(GET, args);
    }

    private static Command handlePut(final String argsString) {
        if (argsString.isEmpty()) {
            throw new InvalidParametersException(PUT.getErrorMessage());
        }

        int firstSpaceIndex = argsString.indexOf(SPACE_DELIMITER);

        if (firstSpaceIndex == -1 || firstSpaceIndex == argsString.length() - 1) {
            throw new InvalidParametersException(PUT.getErrorMessage());
        }

        final String key = argsString.substring(0, firstSpaceIndex);
        final String value = argsString.substring(firstSpaceIndex + 1).strip();

        if (value.equals(TOMBSTONE_VALUE) || value.equals(FLUSH_TOMBSTONE_VALUE)) {
            throw new ReservedKeywordException(String.format("%s is a reserved keyword", value));
        }

        return new Command(PUT, new String[]{key, value});
    }
}
