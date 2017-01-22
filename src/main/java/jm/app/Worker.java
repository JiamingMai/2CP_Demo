package jm.app;

import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;

public class Worker {
    public static void main(String[] args) {
        try {
            ServerSocket proposalSocket = new ServerSocket(20009);
            while (true) {
                Socket client = proposalSocket.accept();
                BufferedReader bi = new BufferedReader(new InputStreamReader(client.getInputStream()));
                String command = bi.readLine();
                PrintWriter out = new PrintWriter(new FileWriter("src/main/resources/log", true));
                out.println(command);
                out.flush();
                out.close();
                System.out.println(command);
                PrintStream resp = new PrintStream(client.getOutputStream());
                resp.println("ack");
                resp.flush();
                bi.close();
                client.close();
            }

        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
