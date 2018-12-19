package br.com.hossomi.kss.kafka.chat.consumer;

import lombok.extern.slf4j.Slf4j;

import java.util.Scanner;
import java.util.function.Consumer;

@Slf4j
public class ListenableScanner {

    private final Scanner scanner;
    private final Consumer<String> listener;

    public ListenableScanner(Scanner scanner, Consumer<String> listener) {
        this.scanner = scanner;
        this.listener = listener;
    }

    public String read() {
        return scanner.nextLine();
    }

    public void readUntilQuit(String quitCommand) {
        while (true) {
            String line = scanner.nextLine();
            if (line.equalsIgnoreCase(quitCommand)) {
                log.debug("Quitting...");
                break;
            }
            listener.accept(line);
        }
    }
}
