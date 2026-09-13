
import java.util.LinkedList;
import java.util.Queue;

class Storage {
    private final Queue<Integer> buffer = new LinkedList<>();
    private final int limit = 5;

    public synchronized void addItem(int value) {
        while (buffer.size() == limit) {
            try {
                wait();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return;
            }
        }

        buffer.offer(value);
        System.out.println("Producer added " + value);

        notifyAll();
    }

    public synchronized int removeItem() {
        while (buffer.isEmpty()) {
            try {
                wait();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return -1;
            }
        }

        int value = buffer.poll();
        System.out.println("Consumer removed " + value);

        notifyAll();
        return value;
    }
}

class Producer implements Runnable {
    private final Storage storage;

    Producer(Storage storage) {
        this.storage = storage;
    }

    public void run() {
        for (int i = 1; i <= 10; i++) {
            storage.addItem(i);

            try {
                Thread.sleep(300);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            }
        }
    }
}

class Consumer implements Runnable {
    private final Storage storage;

    Consumer(Storage storage) {
        this.storage = storage;
    }

    public void run() {
        for (int i = 1; i <= 10; i++) {
            storage.removeItem();

            try {
                Thread.sleep(500);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            }
        }
    }
}

public class ProducerConsumer {
    public static void main(String[] args) {

        Storage storage = new Storage();

        Thread producerThread =
                new Thread(new Producer(storage), "Producer");

        Thread consumerThread =
                new Thread(new Consumer(storage), "Consumer");

        producerThread.start();
        consumerThread.start();

        try {
            producerThread.join();
            consumerThread.join();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }

        System.out.println("Producer and Consumer execution completed  ");
    }
}
