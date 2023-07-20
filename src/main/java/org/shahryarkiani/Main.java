package org.shahryarkiani;

public class Main {
    public static void main(String[] args) {
        if(args.length == 0) {
            var server = new KVServer(8080, 2);
            server.run();
        } else if(args.length == 1) {
            try {
                int port = Integer.parseInt(args[0]);
                var server = new KVServer(port, 2);
                server.run();
            } catch(NumberFormatException e) {
                System.err.println("[ERROR] Unable to parse port number");
                throw new RuntimeException(e);
            }
        } else if(args.length == 2) {
            try {
                int port = Integer.parseInt(args[1]);
                var client = new KVClient(args[0], port);

                client.sendMessage("test", "wow it works");

                Thread.sleep(15 * 1000);

            } catch (NumberFormatException e) {
                System.err.println("[ERROR] Unable to parse port number");
                throw new RuntimeException(e);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }
    }
}