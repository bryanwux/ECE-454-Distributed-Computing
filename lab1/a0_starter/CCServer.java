import javax.xml.crypto.Data;
import java.io.*;
import java.net.*;

class CCServer {
    public static void main(String args[]) throws Exception {
	if (args.length != 1) {
	    System.out.println("usage: java CCServer port");
	    System.exit(-1);
	}
	int port = Integer.parseInt(args[0]);

	ServerSocket ssock = new ServerSocket(port);
	System.out.println("listening on port " + port);
	while(true) {
	    try {
		/*
		  YOUR CODE GOES HERE
		  - accept connection from server socket
		  - read requests from connection repeatedly
		  - for each request, compute an output and send a response
		  - each message has a 4-byte header followed by a payload
		  - the header is the length of the payload
		    (signed, two's complement, big-endian)
		  - the payload is a string
		    (UTF-8, big-endian)
		*/
		Socket csock = ssock.accept();
		System.out.println("Just connect to " + csock.getRemoteSocketAddress());
		DataInputStream din = new DataInputStream(csock.getInputStream());
		// read the first four bytes of the input stream, which indicates the size of the payload
		final int size = din.readInt();
		System.out.println("The size of the data payload is: " + size);
		final byte[] inBytes = new byte[size];
		din.readFully(inBytes);

		UnionFind graph = new UnionFind();

		int i = 0;
		while(i < inBytes.length){
			int edge_i = 0;
			while(inBytes[i] != 32){
				char c = (char) inBytes[i];
				edge_i = edge_i * 10 + Character.getNumericValue(c);
				i++;
			}
			i++;

			int edge_j = 0;
			while(inBytes[i] != 10){
				char c = (char) inBytes[i];
				edge_j = edge_j * 10 + Character.getNumericValue(c);
				i++;
			}
			i++;

			graph.union(edge_i, edge_j);
		}

		DataOutputStream dout = new DataOutputStream(csock.getOutputStream());
		StringBuilder output = new StringBuilder();
		for(int edge: graph.keySet()){
			output.append(edge).append(" ").append(graph.find(edge)).append("\n");
		}

		byte[] response = output.toString().getBytes();
		dout.writeInt(response.length);
		dout.write(response);
		dout.flush();
		dout.close();

	    } catch (Exception e) {
		e.printStackTrace();
	    }
	}
    }
}
