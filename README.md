# How to Run the Persistent Key-Value Store

Hey there! This project is a simple, persistent key-value store. It's built with Python's standard library and uses a log-structured design, which makes it great for fast writes and reliable data storage.

To get started, you'll need to create two files: `server.py` and `client.py`.

## On Linux or macOS

1. **Save the files:** Create a `server.py` file and a `client.py` file with the code provided.

2. **Open two separate Terminal windows.**

3. **In the first Terminal, start the server:**

```
python3 server.py
```

You should see "Server started on localhost:9999". This terminal will now be busy running the server.

4. **In the second Terminal, run the client to test it:**

```
python3 client.py
```

The client will send a series of commands to the server, and you'll see the results printed to the console.

## On Windows

The process is pretty much the same as on Linux/macOS, just a couple of small differences in the commands.

1. **Save the files:** Create `server.py` and `client.py` with the provided code.

2. **Open two Command Prompt or PowerShell windows.**

3. **In the first window, start the server:**

```
python server.py
```

4. **In the second window, run the client:**

```
python client.py
```

## Testing

To run tests, I have included a test file: `server_tests.py`

Simply run `puthon3 server_tests.py`

This will verify that all methods in the server are working as expected.
