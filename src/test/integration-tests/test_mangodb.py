import pytest
import socket
import time

HOST = 'localhost'
PORT = 8080  # Port used in MangoDBServer.java

def send_command(command):
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        try:
            s.connect((HOST, PORT))
            s.sendall(f"{command}\n".encode("utf-8"))
            response_bytes = s.recv(1024) # why 1024?
            response = response_bytes.decode("utf-8")
        except ConnectionRefusedError:
            pytest.fail(f"Connection to {HOST}:{PORT} failed")
        except Exception as e:
            pytest.fail(f"Unexpected error: {e}")

        return response

# -- Test Cases ---

def test_put_get():
    assert send_command("PUT hi here") == "OK\n"
    assert send_command("GET hi") == "here\n"

def test_put_get_multiple():
    assert send_command("PUT hi here") == "OK\n"
    assert send_command("PUT hi here_too") == "OK\n"
    assert send_command("GET hi") == "here_too\n"

def test_get_nonexistent():
    assert send_command("GET higurlkjdiutlwqke") == "NOT FOUND\n"

def test_invalid_command():
    assert send_command("MAY hi") == "INVALID INPUT\n"
    assert send_command("PUT hi here how") == "INVALID INPUT\n"
    assert send_command("GET hi here") == "INVALID INPUT\n"




