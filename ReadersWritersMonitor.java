import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.ThreadLocalRandom;

public class ReadersWritersMonitor {

    static final int NUMBER_OF_READERS = 4;
    static final int NUMBER_OF_WRITERS = 2;

    static final int READS_PER_READER = 2;
    static final int WRITES_PER_WRITER = 2;

    static int sharedValue = 0;

    static final Object stateLock = new Object();

    static final Map<String, String> readerStates = new LinkedHashMap<>();
    static final Map<String, String> writerStates = new LinkedHashMap<>();

    static final List<Event> events = new ArrayList<>();

    static final long startTime = System.nanoTime();


    // -----------------------------------------------------
    // Monitor
    // -----------------------------------------------------

    static class ReadersWritersController {

        private int activeReaders = 0;
        private int waitingWriters = 0;
        private boolean writerActive = false;

        public synchronized void beginRead() throws InterruptedException {
            /*
             * Writer preference:
             * if a writer is active OR waiting,
             * new readers wait.
             */
            while (writerActive || waitingWriters > 0) {
                wait();
            }

            activeReaders++;
        }

        public synchronized void endRead() {
            activeReaders--;

            if (activeReaders == 0) {
                notifyAll();
            }
        }

        public synchronized void beginWrite() throws InterruptedException {
            waitingWriters++;

            try {
                while (writerActive || activeReaders > 0) {
                    wait();
                }

                writerActive = true;
            } finally {
                waitingWriters--;
            }
        }

        public synchronized void endWrite() {
            writerActive = false;
            notifyAll();
        }

        public synchronized int getActiveReaders() {
            return activeReaders;
        }

        public synchronized int getWaitingWriters() {
            return waitingWriters;
        }

        public synchronized boolean isWriterActive() {
            return writerActive;
        }
    }

    static final ReadersWritersController monitor = new ReadersWritersController();


    // -----------------------------------------------------
    // Event
    // -----------------------------------------------------

    static class Event {
        int number;
        double time;
        String message;
        int sharedValue;
        int activeReaders;
        int waitingWriters;
        boolean writerActive;
        Map<String, String> readerStates;
        Map<String, String> writerStates;

        Event(int number, double time, String message, int sharedValue,
              int activeReaders, int waitingWriters, boolean writerActive,
              Map<String, String> readerStates, Map<String, String> writerStates) {
            this.number = number;
            this.time = time;
            this.message = message;
            this.sharedValue = sharedValue;
            this.activeReaders = activeReaders;
            this.waitingWriters = waitingWriters;
            this.writerActive = writerActive;
            this.readerStates = new LinkedHashMap<>(readerStates);
            this.writerStates = new LinkedHashMap<>(writerStates);
        }
    }


    static void randomSleep(int min, int max) {
        try {
            Thread.sleep(ThreadLocalRandom.current().nextInt(min, max + 1));
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }


    static void recordEvent(String message) {
        synchronized (stateLock) {
            double time = (System.nanoTime() - startTime) / 1_000_000_000.0;

            Event event = new Event(
                    events.size() + 1,
                    time,
                    message,
                    sharedValue,
                    monitor.getActiveReaders(),
                    monitor.getWaitingWriters(),
                    monitor.isWriterActive(),
                    readerStates,
                    writerStates
            );

            events.add(event);

            System.out.printf("[%6.3fs] %-42s | readers=%d | waiting writers=%d | value=%d%n",
                    time, message, event.activeReaders, event.waitingWriters, sharedValue);
        }
    }


    // -----------------------------------------------------
    // Reader
    // -----------------------------------------------------

    static class Reader implements Runnable {
        int id;

        Reader(int id) {
            this.id = id;
        }

        @Override
        public void run() {
            String name = "R" + id;

            try {
                for (int round = 1; round <= READS_PER_READER; round++) {
                    randomSleep(200, 700);

                    synchronized (stateLock) {
                        readerStates.put(name, "Waiting");
                    }
                    recordEvent(name + " requested permission to read");

                    monitor.beginRead();

                    synchronized (stateLock) {
                        readerStates.put(name, "Reading");
                    }
                    recordEvent(name + " entered reading section");

                    int valueSeen = sharedValue;
                    randomSleep(450, 850);
                    recordEvent(name + " read value " + valueSeen);

                    monitor.endRead();

                    synchronized (stateLock) {
                        readerStates.put(name, "Finished Reading");
                    }
                    recordEvent(name + " finished reading");
                }

                synchronized (stateLock) {
                    readerStates.put(name, "Completed");
                }
                recordEvent(name + " completed all reads");

            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }


    // -----------------------------------------------------
    // Writer
    // -----------------------------------------------------

    static class Writer implements Runnable {
        int id;

        Writer(int id) {
            this.id = id;
        }

        @Override
        public void run() {
            String name = "W" + id;

            try {
                for (int round = 1; round <= WRITES_PER_WRITER; round++) {
                    randomSleep(300, 800);

                    synchronized (stateLock) {
                        writerStates.put(name, "Waiting");
                    }
                    recordEvent(name + " requested permission to write");

                    monitor.beginWrite();

                    synchronized (stateLock) {
                        writerStates.put(name, "Writing");
                    }
                    recordEvent(name + " entered writing section");

                    int oldValue = sharedValue;
                    randomSleep(500, 900);
                    sharedValue++;

                    recordEvent(name + " changed value " + oldValue + " -> " + sharedValue);

                    monitor.endWrite();

                    synchronized (stateLock) {
                        writerStates.put(name, "Finished Writing");
                    }
                    recordEvent(name + " left writing section");
                }

                synchronized (stateLock) {
                    writerStates.put(name, "Completed");
                }
                recordEvent(name + " completed all writes");

            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }


    // -----------------------------------------------------
    // JSON
    // -----------------------------------------------------

    static String escape(String text) {
        return text.replace("\\", "\\\\")
                .replace("\"", "\\\"")
                .replace("\n", "\\n");
    }


    static String mapJson(Map<String, String> map) {
        StringBuilder result = new StringBuilder("{");
        int index = 0;

        for (Map.Entry<String, String> entry : map.entrySet()) {
            if (index++ > 0) {
                result.append(",");
            }
            result.append("\"").append(escape(entry.getKey())).append("\":\"")
                    .append(escape(entry.getValue())).append("\"");
        }

        result.append("}");
        return result.toString();
    }


    static String eventsJson() {
        StringBuilder result = new StringBuilder("[");

        for (int i = 0; i < events.size(); i++) {
            Event e = events.get(i);

            if (i > 0) {
                result.append(",");
            }

            result.append("{")
                    .append("\"number\":").append(e.number).append(",")
                    .append("\"time\":").append(String.format(Locale.US, "%.3f", e.time)).append(",")
                    .append("\"message\":\"").append(escape(e.message)).append("\",")
                    .append("\"sharedValue\":").append(e.sharedValue).append(",")
                    .append("\"activeReaders\":").append(e.activeReaders).append(",")
                    .append("\"waitingWriters\":").append(e.waitingWriters).append(",")
                    .append("\"writerActive\":").append(e.writerActive).append(",")
                    .append("\"readerStates\":").append(mapJson(e.readerStates)).append(",")
                    .append("\"writerStates\":").append(mapJson(e.writerStates))
                    .append("}");
        }

        result.append("]");
        return result.toString();
    }


    // -----------------------------------------------------
    // HTML
    // -----------------------------------------------------

    static void generateHTML() throws IOException {
        String html = """
<!DOCTYPE html>
<html>
<head>
<meta charset="UTF-8">
<title>Readers Writers Monitor</title>

<style>
body {
    font-family: Arial, sans-serif;
    background: #eef2f7;
    padding: 30px;
}

main {
    max-width: 1050px;
    margin: auto;
}

h1 {
    text-align: center;
}

.panel {
    background: white;
    padding: 20px;
    border-radius: 12px;
    margin: 15px 0;
}

.grid {
    display: grid;
    grid-template-columns: repeat(3, 1fr);
    gap: 15px;
}

.card {
    background: white;
    border: 1px solid #ddd;
    padding: 18px;
    text-align: center;
    border-radius: 10px;
}

.controls {
    text-align: center;
    margin: 25px;
}

button {
    padding: 10px 16px;
    margin: 4px;
}

#message {
    background: #111827;
    color: white;
}
</style>
</head>

<body>

<main>

<h1>Readers-Writers Monitor Simulation</h1>

<div class="panel">
<p>Event: <strong id="event"></strong></p>
<p>Time: <strong id="time"></strong> seconds</p>
<p>Shared value: <strong id="value"></strong></p>
<p>Active readers: <strong id="activeReaders"></strong></p>
<p>Waiting writers: <strong id="waitingWriters"></strong></p>
<p>Writer active: <strong id="writerActive"></strong></p>
</div>

<h2>Readers</h2>
<div id="readerArea" class="grid"></div>

<h2>Writers</h2>
<div id="writerArea" class="grid"></div>

<div class="controls">
<button onclick="previous()">Previous</button>
<button onclick="play()">Play</button>
<button onclick="pause()">Pause</button>
<button onclick="next()">Next</button>
<button onclick="resetSimulation()">Reset</button>
</div>

<div class="panel" id="message"></div>

</main>


<script>

const events = __EVENT_DATA__;

let index = 0;
let timer = null;

function render() {
    const e = events[index];

    document.getElementById("event").textContent = e.number + " / " + events.length;
    document.getElementById("time").textContent = e.time.toFixed(3);
    document.getElementById("value").textContent = e.sharedValue;
    document.getElementById("activeReaders").textContent = e.activeReaders;
    document.getElementById("waitingWriters").textContent = e.waitingWriters;
    document.getElementById("writerActive").textContent = e.writerActive ? "Yes" : "No";
    document.getElementById("message").textContent = e.message;

    const readerArea = document.getElementById("readerArea");
    readerArea.innerHTML = "";

    Object.entries(e.readerStates).forEach(([name, state]) => {
        readerArea.innerHTML += `
            <div class="card">
                <h3>${name}</h3>
                <strong>${state}</strong>
            </div>
        `;
    });

    const writerArea = document.getElementById("writerArea");
    writerArea.innerHTML = "";

    Object.entries(e.writerStates).forEach(([name, state]) => {
        writerArea.innerHTML += `
            <div class="card">
                <h3>${name}</h3>
                <strong>${state}</strong>
            </div>
        `;
    });
}

function next() {
    if (index < events.length - 1) {
        index++;
        render();
    }
}

function previous() {
    if (index > 0) {
        index--;
        render();
    }
}

function play() {
    if (timer !== null) {
        return;
    }

    timer = setInterval(() => {
        if (index < events.length - 1) {
            index++;
            render();
        } else {
            pause();
        }
    }, 850);
}

function pause() {
    clearInterval(timer);
    timer = null;
}

function resetSimulation() {
    pause();
    index = 0;
    render();
}

render();

</script>

</body>
</html>
""";

        html = html.replace("__EVENT_DATA__", eventsJson());

        Path folder = Paths.get("generated_html");
        Files.createDirectories(folder);

        Path output = folder.resolve("readers_writers_monitor.html");
        Files.writeString(output, html, StandardCharsets.UTF_8);

        System.out.println("\nHTML generated: " + output.toAbsolutePath());
    }


    // -----------------------------------------------------
    // Main
    // -----------------------------------------------------

    public static void main(String[] args) throws Exception {
        for (int i = 1; i <= NUMBER_OF_READERS; i++) {
            readerStates.put("R" + i, "Not Started");
        }

        for (int i = 1; i <= NUMBER_OF_WRITERS; i++) {
            writerStates.put("W" + i, "Not Started");
        }

        recordEvent("Monitor simulation started");

        List<Thread> threads = new ArrayList<>();

        for (int i = 1; i <= NUMBER_OF_READERS; i++) {
            threads.add(new Thread(new Reader(i), "Reader-" + i));
        }

        for (int i = 1; i <= NUMBER_OF_WRITERS; i++) {
            threads.add(new Thread(new Writer(i), "Writer-" + i));
        }

        Collections.shuffle(threads);

        for (Thread thread : threads) {
            thread.start();
        }

        for (Thread thread : threads) {
            thread.join();
        }

        recordEvent("All monitor threads completed");

        generateHTML();

        System.out.println("\nFinal shared value = " + sharedValue);
    }
}
