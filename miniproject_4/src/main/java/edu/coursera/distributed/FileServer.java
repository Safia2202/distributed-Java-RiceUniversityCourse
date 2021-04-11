package edu.coursera.distributed;

import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.file.Files;

/**
 * A basic and very limited implementation of a file server that responds to GET
 * requests from HTTP clients.
 */
public final class FileServer {
    /**
     * Main entrypoint for the basic file server.
     *
     * @param socket Provided socket to accept connections on.
     * @param fs A proxy filesystem to serve files from. See the PCDPFilesystem
     *           class for more detailed documentation of its usage.
     * @param ncores The number of cores that are available to your
     *               multi-threaded file server. Using this argument is entirely
     *               optional. You are free to use this information to change
     *               how you create your threads, or ignore it.
     * @throws IOException If an I/O error is detected on the server. This
     *                     should be a fatal error, your file server
     *                     implementation is not expected to ever throw
     *                     IOExceptions during normal operation.
     */
    public void run(final ServerSocket socket, final PCDPFilesystem fs,
                    final int ncores) throws IOException {
        /*
         * Enter a spin loop for handling client requests to the provided
         * ServerSocket object.
         */
        while (true) {
            Socket s = socket.accept();

            Thread thread = new Thread(() -> {
                try {
                    InputStream stream = s.getInputStream();
                    InputStreamReader reader = new InputStreamReader(stream);
                    BufferedReader bufferedReader = new BufferedReader(reader);

                    String line = bufferedReader.readLine();
                    assert line != null;
                    assert line.startsWith("GET");
                    final String path = line.split(" ")[1];

                    PCDPPath pcdpPath = new PCDPPath(path);
                    String fileContent = fs.readFile(pcdpPath);

                    OutputStream out = s.getOutputStream();
                    PrintWriter printWriter = new PrintWriter(out);

                    if(fileContent != null) {
                        printWriter.write("HTTP/1.0 200 OK\r\n");
                        printWriter.write("Server: FileServer\r\n");
                        printWriter.write("\r\n");
                        printWriter.write(fileContent + "\r\n");
                    } else {
                        printWriter.write("HTTP/1.0 404 Not Found\r\n");
                        printWriter.write("Server: FileServer\r\n");
                        printWriter.write("\r\n");
                    }

                    printWriter.close();
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            });

            thread.start();
        }
    }
}
