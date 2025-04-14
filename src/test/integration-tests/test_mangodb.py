import pytest
import socket
import time

HOST = 'localhost'
PORT = 8080  # Port used in MangoDBServer.java

def send_command(command):
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        try:
            # Set timeout to prevent tests from hanging
            s.settimeout(10)
            s.connect((HOST, PORT))
            s.sendall(f"{command}\n".encode("utf-8"))

            # Using 1024 as standard buffer size for receiving input
            response_bytes = s.recv(1024)
            response = response_bytes.decode("utf-8")
        except ConnectionRefusedError:
            pytest.fail(f"Connection to {HOST}:{PORT} failed")
        except socket.timeout:
                pytest.fail(f"Connection to {HOST}:{PORT} timed out")
        except Exception as e:
            pytest.fail(f"Unexpected error: {e}")

        return response

# -- Test Cases ---

def test_put_get():
    assert send_command("PUT key1 value1") == "OK\n"
    assert send_command("GET key1") == "value1\n"

def test_put_get_multiple():
    assert send_command("PUT key1 value1") == "OK\n"
    assert send_command("PUT key1 value2") == "OK\n"
    assert send_command("GET key1") == "value2\n"

def test_get_nonexistent():
    assert send_command("GET random_234123652346") == "NOT FOUND\n"

def test_invalid_command():
    assert send_command("FLUSH") == "INVALID INPUT\n"
    assert send_command("MAY key1") == "INVALID INPUT\n"

def test_attempt_writing_tombstone():
    assert send_command("PUT key1 __TOMBSTONE__") == "RESERVED KEYWORD __TOMBSTONE__\n"

def test_delete_nonexistent():
    assert send_command("DELETE random_1234123412") == "NOT FOUND\n"

def test_delete_key():
    assert send_command("PUT key3 value3") == "OK\n"
    assert send_command("DELETE key3") == "OK\n"
    assert send_command("DELETE key3") == "NOT FOUND\n"
