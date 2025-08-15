import socket
import threading
import time
import json
from server import ThreadedTCPServer, ThreadedTCPRequestHandler, KeyValueStore, HOST, PORT

def find_free_port():
    """
    Finds and returns a free port by trying to bind to a temporary socket.
    This helps to avoid "Address already in use" errors.
    """
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(('', 0))
        return s.getsockname()[1]

def send_command(port, command):
    """A helper function to send a command to the server."""
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        # A new socket is created to handle the connection for each command.
        try:
            sock.connect((HOST, port))
            sock.sendall(command.encode('utf-8'))
            received = str(sock.recv(1024), "utf-8")
            return received.strip()
        except Exception as e:
            # If the server is already shut down, we'll get an error.
            # We can ignore this, as it's part of the shutdown process.
            return f"ERROR: {e}"

def run_tests():
    """
    This function runs all the tests in a simple, sequential manner.
    It starts the server, runs the tests, and then shuts down the server.
    This approach guarantees the script will exit cleanly.
    """
    print("Running manual tests...")
    
    # 1. Start the server on a new thread with a free port
    port = find_free_port()
    kv_store = KeyValueStore()
    server = ThreadedTCPServer((HOST, port), ThreadedTCPRequestHandler, kv_store)
    server_thread = threading.Thread(target=server.serve_forever, daemon=True)
    server_thread.start()

    # Wait for the server to be ready
    time.sleep(1) # a simple sleep is sufficient for this simple test

    # 2. Run the tests
    try:
        # Test PUT and READ
        print("Test 1: PUT and READ.")
        response = send_command(port, "PUT test_key test_value")
        assert response == "OK", f"Expected 'OK', got '{response}'"
        response = send_command(port, "READ test_key")
        assert response == "OK test_value", f"Expected 'OK test_value', got '{response}'"
        print("Test 1: PUT and READ passed.")

        # Test overwrite
        print("Test 2: Overwrite.")
        response = send_command(port, "PUT key_to_overwrite old_value")
        assert response == "OK", f"Expected 'OK', got '{response}'"
        response = send_command(port, "PUT key_to_overwrite new_value")
        assert response == "OK", f"Expected 'OK', got '{response}'"
        response = send_command(port, "READ key_to_overwrite")
        assert response == "OK new_value", f"Expected 'OK new_value', got '{response}'"
        print("Test 2: Overwrite passed.")
        
        # Test DELETE
        print("Test 3: DELETE.")
        response = send_command(port, "PUT key_to_delete value")
        assert response == "OK", f"Expected 'OK', got '{response}'"
        response = send_command(port, "DELETE key_to_delete")
        assert response == "OK", f"Expected 'OK', got '{response}'"
        time.sleep(1)
        response = send_command(port, "READ key_to_delete")
        assert response == "OK NULL", f"Expected 'OK NULL', got '{response}'"
        print("Test 3: DELETE passed.")
        
        # Test nonexistent key
        print("Test 4: Non-existent key.")
        response = send_command(port, "READ non_existent_key")
        assert response == "OK NULL", f"Expected 'OK NULL', got '{response}'"
        print("Test 4: Non-existent key passed.")

        # Test READRANGE
        print("Test 5: READRANGE.")
        send_command(port, "PUT a_key value_a")
        send_command(port, "PUT b_key value_b")
        send_command(port, "PUT c_key value_c")
        response = send_command(port, "READRANGE b_key c_key")
        status, data = response.split(' ', 1)
        assert status == "OK", f"Expected 'OK', got '{status}'"
        results = json.loads(data)
        expected_results = [["b_key", "value_b"], ["c_key", "value_c"]]
        results.sort()
        expected_results.sort()
        assert results == expected_results, f"Expected {expected_results}, got {results}"
        print("Test 5: READRANGE passed.")

        print("\nAll tests passed successfully! 🎉")

    except AssertionError as e:
        print(f"\nTEST FAILED: {e}")
    finally:
        # 3. Shut down the server and exit
        print("Shutting down the server...")
        server.shutdown()
        server_thread.join()
        print("Server shut down. The script will now exit.")

if __name__ == '__main__':
    run_tests()
