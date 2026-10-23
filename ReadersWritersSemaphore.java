import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.Semaphore;
import java.util.concurrent.ThreadLocalRandom;

public class ReadersWritersSemaphore {

    static final int NUMBER_OF_READERS = 4;
    static final int NUMBER_OF_WRITERS = 2;
    static final int READS_PER_READER = 2;
    static final int WRITES_PER_WRITER = 2;

    static int sharedValue = 0;
    static int activeReaders = 0;

    // Protects activeReaders
    static final Semaphore readerCountLock = new Semaphore(1, true);

    // Controls the actual shared resource
    static final Semaphore resource = new Semaphore(1, true);

    // Readers and writers enter through the same queue.
    // Fair mode helps reduce starvation.
    static final Semaphore serviceQueue = new Semaphore(1, true);

    // Only used for keeping event/state information consistent
    static final Object stateLock = new Object();

    static final Map<String, String> readerStates = new LinkedHashMap<>();
    static final Map<String, String> writerStates = new LinkedHashMap<>();

    static final List<Event> events = new ArrayList<>();

    static final long startTime = System.nanoTime();


    static class Event {
        int number;
        double time;
        String message;
        int sharedValue;
        int activeReaders;
        Map<String, String> readerStates;
        Map<String, String> writerStates;

        Event(int number, double time, String message, int sharedValue, int activeReaders,
              Map<String, String> readerStates, Map<String, String> writerStates) {
            this.number = number;
            this.time = time;
            this.message = message;
            this.sharedValue = sharedValue;
            this.activeReaders = activeReaders;
            this.readerStates = new LinkedHashMap<>(readerStates);
            this.writerStates = new LinkedHashMap<>(writerStates);
        }
    }


    static void randomSleep(int min, int max) {
        try {
            int delay = ThreadLocalRandom.current().nextInt(min, max + 1);
            Thread.sleep(delay);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }


    static void recordEvent(String message) {
        synchronized (stateLock) {
            double elapsed = (System.nanoTime() - startTime) / 1_000_000_000.0;

            Event event = new Event(
                    events.size() + 1,
                    elapsed,
                    message,
                    sharedValue,
                    activeReaders,
                    readerStates,
                    writerStates
            );

            events.add(event);

            System.out.printf("[%6.3fs] %-42s | readers=%d | value=%d%n",
                    elapsed, message, activeReaders, sharedValue);
        }
    }


    // -----------------------------------------------------
    // Reader
    // -----------------------------------------------------

    static class Reader implements Runnable {
        private final int id;

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

                    recordEvent(name + " wants to read");

                    /*
                     * Join the common service queue.
                     */
                    serviceQueue.acquire();

                    /*
                     * Safely change activeReaders.
                     */
                    readerCountLock.acquire();

                    activeReaders++;

                    /*
                     * First reader locks the resource.
                     */
                    if (activeReaders == 1) {
                        resource.acquire();
                    }

                    readerCountLock.release();
                    serviceQueue.release();

                    synchronized (stateLock) {
                        readerStates.put(name, "Reading");
                    }

                    recordEvent(name + " entered reading section");

                    int valueSeen = sharedValue;

                    randomSleep(500, 900);

                    recordEvent(name + " read shared value " + valueSeen);

                    /*
                     * Reader leaves.
                     */
                    readerCountLock.acquire();

                    activeReaders--;

                    /*
                     * Last reader makes resource
                     * available to writers.
                     */
                    if (activeReaders == 0) {
                        resource.release();
                    }

                    readerCountLock.release();

                    synchronized (stateLock) {
                        readerStates.put(name, "Finished Reading");
                    }

                    recordEvent(name + " left reading section");
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
        private final int id;

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

                    recordEvent(name + " wants to write");

                    serviceQueue.acquire();

                    /*
                     * Writer needs exclusive access
                     * to the resource.
                     */
                    resource.acquire();

                    serviceQueue.release();

                    synchronized (stateLock) {
                        writerStates.put(name, "Writing");
                    }

                    recordEvent(name + " entered writing section");

                    int oldValue = sharedValue;

                    randomSleep(500, 900);

                    sharedValue++;

                    recordEvent(name + " changed value " + oldValue + " -> " + sharedValue);

                    synchronized (stateLock) {
                        writerStates.put(name, "Releasing");
                    }

                    recordEvent(name + " is releasing resource");

                    resource.release();

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
    // JSON helpers
    // -----------------------------------------------------

    static String escape(String text) {
        return text.replace("\\", "\\\\")
                .replace("\"", "\\\"")
                .replace("\n", "\\n");
    }


    static String mapToJson(Map<String, String> map) {
        StringBuilder json = new StringBuilder("{");
        int count = 0;

        for (Map.Entry<String, String> entry : map.entrySet()) {
            if (count++ > 0) {
                json.append(",");
            }
            json.append("\"")
                    .append(escape(entry.getKey()))
                    .append("\":\"")
                    .append(escape(entry.getValue()))
                    .append("\"");
        }

        json.append("}");
        return json.toString();
    }


    static String eventsToJson() {
        StringBuilder json = new StringBuilder("[");

        for (int i = 0; i < events.size(); i++) {
            Event e = events.get(i);

            if (i > 0) {
                json.append(",");
            }

            json.append("{");
            json.append("\"number\":").append(e.number).append(",");
            json.append("\"time\":").append(String.format(Locale.US, "%.3f", e.time)).append(",");
            json.append("\"message\":\"").append(escape(e.message)).append("\",");
            json.append("\"sharedValue\":").append(e.sharedValue).append(",");
            json.append("\"activeReaders\":").append(e.activeReaders).append(",");
            json.append("\"readerStates\":").append(mapToJson(e.readerStates)).append(",");
            json.append("\"writerStates\":").append(mapToJson(e.writerStates));
            json.append("}");
        }

        json.append("]");
        return json.toString();
    }


    // -----------------------------------------------------
    // HTML generator
    // -----------------------------------------------------

    static void generateHTML() throws IOException {
        String data = eventsToJson();

        String html = """
<!DOCTYPE html>
<html>
<head>
<meta charset="UTF-8">
<title>Readers Writers - Semaphore</title>

<style>
body {
    font-family: Arial, sans-serif;
    background: #f4f6f9;
    margin: 0;
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
    margin: 15px 0;
    border-radius: 12px;
}

.grid {
    display: grid;
    grid-template-columns: repeat(3, 1fr);
    gap: 15px;
}

.card {
    background: white;
    padding: 18px;
    text-align: center;
    border-radius: 10px;
    border: 1px solid #ddd;
}

.controls {
    text-align: center;
    margin: 25px;
}

button {
    padding: 10px 17px;
    margin: 4px;
    cursor: pointer;
}

#message {
    background: #111827;
    color: white;
}

.value {
    font-size: 28px;
    font-weight: bold;
}
</style>
</head>

<body>

<main>

<h1>Readers-Writers Semaphore Simulation</h1>

<div class="panel">
<p>Event: <strong id="eventNumber"></strong></p>
<p>Execution time: <strong id="time"></strong> seconds</p>
<p>Shared value: <span class="value" id="sharedValue"></span></p>
<p>Active readers: <strong id="activeReaders"></strong></p>
</div>

<h2>Readers</h2>
<div class="grid" id="readerArea"></div>

<h2>Writers</h2>
<div class="grid" id="writerArea"></div>

<div class="controls">
<button onclick="previousEvent()">Previous</button>
<button onclick="play()">Play</button>
<button onclick="pause()">Pause</button>
<button onclick="nextEvent()">Next</button>
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

    document.getElementById("eventNumber").textContent = e.number + " / " + events.length;
    document.getElementById("time").textContent = e.time.toFixed(3);
    document.getElementById("sharedValue").textContent = e.sharedValue;
    document.getElementById("activeReaders").textContent = e.activeReaders;
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

function nextEvent() {
    if (index < events.length - 1) {
        index++;
        render();
    }
}

function previousEvent() {
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

        html = html.replace("__EVENT_DATA__", data);

        Path folder = Paths.get("generated_html");
        Files.createDirectories(folder);

        Path output = folder.resolve("readers_writers_semaphore.html");
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

        recordEvent("Semaphore simulation started");

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

        recordEvent("All reader and writer threads completed");

        generateHTML();

        System.out.println("\nFinal shared value = " + sharedValue);
    }
}
