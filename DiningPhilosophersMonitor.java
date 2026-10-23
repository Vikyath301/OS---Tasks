import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.ThreadLocalRandom;

public class DiningPhilosophersMonitor {

    static final int PHILOSOPHER_COUNT = 4;
    static final int MEALS_PER_PHILOSOPHER = 3;

    static final Object stateLock = new Object();

    static final String[] philosopherStates = new String[PHILOSOPHER_COUNT];
    static final int[] meals = new int[PHILOSOPHER_COUNT];

    static final List<Event> events = new ArrayList<>();

    static final long startTime = System.nanoTime();


    // -----------------------------------------------------
    // Monitor
    // -----------------------------------------------------

    static class DiningMonitor {

        /*
         * -1 = available
         * otherwise philosopher ID
         */
        private final int[] forkOwners = new int[PHILOSOPHER_COUNT];

        DiningMonitor() {
            Arrays.fill(forkOwners, -1);
        }

        /*
         * Philosopher waits until BOTH forks
         * are available.
         *
         * Both forks are then allocated while
         * the monitor lock is held.
         */
        public synchronized void takeForks(int philosopher) throws InterruptedException {
            int left = philosopher;
            int right = (philosopher + 1) % PHILOSOPHER_COUNT;

            while (forkOwners[left] != -1 || forkOwners[right] != -1) {
                wait();
            }

            forkOwners[left] = philosopher;
            forkOwners[right] = philosopher;
        }

        /*
         * Both forks are released together.
         */
        public synchronized void releaseForks(int philosopher) {
            int left = philosopher;
            int right = (philosopher + 1) % PHILOSOPHER_COUNT;

            forkOwners[left] = -1;
            forkOwners[right] = -1;

            /*
             * Wake philosophers so they can
             * recheck fork availability.
             */
            notifyAll();
        }

        public synchronized int[] getForkOwners() {
            return forkOwners.clone();
        }
    }

    static final DiningMonitor monitor = new DiningMonitor();


    // -----------------------------------------------------
    // Event
    // -----------------------------------------------------

    static class Event {
        int number;
        double time;
        String message;
        String[] states;
        int[] forkOwners;
        int[] meals;

        Event(int number, double time, String message,
              String[] states, int[] forkOwners, int[] meals) {
            this.number = number;
            this.time = time;
            this.message = message;
            this.states = states.clone();
            this.forkOwners = forkOwners.clone();
            this.meals = meals.clone();
        }
    }


    static void randomSleep(int minimum, int maximum) {
        try {
            Thread.sleep(ThreadLocalRandom.current().nextInt(minimum, maximum + 1));
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
                    philosopherStates,
                    monitor.getForkOwners(),
                    meals
            );

            events.add(event);

            System.out.printf("[%6.3fs] %s%n", elapsed, message);
        }
    }


    // -----------------------------------------------------
    // Philosopher
    // -----------------------------------------------------

    static class Philosopher implements Runnable {
        private final int id;

        Philosopher(int id) {
            this.id = id;
        }

        @Override
        public void run() {
            try {
                for (int round = 1; round <= MEALS_PER_PHILOSOPHER; round++) {
                    synchronized (stateLock) {
                        philosopherStates[id] = "Thinking";
                    }
                    recordEvent("P" + id + " is thinking");

                    randomSleep(450, 1000);

                    synchronized (stateLock) {
                        philosopherStates[id] = "Hungry / Waiting";
                    }
                    recordEvent("P" + id + " became hungry");

                    /*
                     * This may call wait()
                     * inside the monitor.
                     */
                    monitor.takeForks(id);

                    synchronized (stateLock) {
                        philosopherStates[id] = "Acquiring Forks";
                    }
                    recordEvent("P" + id + " obtained both forks");

                    synchronized (stateLock) {
                        philosopherStates[id] = "Eating";
                    }
                    recordEvent("P" + id + " started eating");

                    randomSleep(550, 1000);

                    synchronized (stateLock) {
                        meals[id]++;
                    }
                    recordEvent("P" + id + " completed meal " + meals[id]);

                    synchronized (stateLock) {
                        philosopherStates[id] = "Releasing Forks";
                    }
                    recordEvent("P" + id + " is releasing both forks");

                    monitor.releaseForks(id);

                    recordEvent("P" + id + " released both forks");
                }

                synchronized (stateLock) {
                    philosopherStates[id] = "Completed";
                }
                recordEvent("P" + id + " completed all meals");

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


    static String statesJson(String[] values) {
        StringBuilder result = new StringBuilder("[");

        for (int i = 0; i < values.length; i++) {
            if (i > 0) {
                result.append(",");
            }
            result.append("\"").append(escape(values[i])).append("\"");
        }

        result.append("]");
        return result.toString();
    }


    static String intArrayJson(int[] values) {
        StringBuilder result = new StringBuilder("[");

        for (int i = 0; i < values.length; i++) {
            if (i > 0) {
                result.append(",");
            }
            result.append(values[i]);
        }

        result.append("]");
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
                    .append("\"states\":").append(statesJson(e.states)).append(",")
                    .append("\"forkOwners\":").append(intArrayJson(e.forkOwners)).append(",")
                    .append("\"meals\":").append(intArrayJson(e.meals))
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
<title>Dining Philosophers Monitor</title>

<style>
body {
    font-family: Arial, sans-serif;
    background: #f4f6f8;
    padding: 30px;
}

main {
    max-width: 1100px;
    margin: auto;
}

h1 {
    text-align: center;
}

.table {
    background: white;
    padding: 25px;
    border-radius: 20px;
}

.philosophers,
.forks {
    display: grid;
    grid-template-columns: repeat(4, 1fr);
    gap: 15px;
}

.philosopher {
    background: #ecfdf5;
    padding: 20px;
    border-radius: 12px;
    text-align: center;
}

.fork {
    background: white;
    border: 2px solid #ddd;
    padding: 15px;
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
    cursor: pointer;
}

.log {
    background: #111827;
    color: white;
    padding: 20px;
    border-radius: 10px;
}
</style>
</head>

<body>

<main>

<h1>Dining Philosophers - Monitor Simulation</h1>

<div class="table">
<h2>Philosophers</h2>
<div id="philosophers" class="philosophers"></div>

<h2>Fork Ownership</h2>
<div id="forks" class="forks"></div>
</div>

<div class="controls">
<button onclick="previous()">Previous</button>
<button onclick="play()">Play</button>
<button onclick="pause()">Pause</button>
<button onclick="next()">Next</button>
<button onclick="resetSimulation()">Reset</button>
</div>

<div class="log">
<p>Event: <strong id="event"></strong></p>
<p>Time: <strong id="time"></strong> seconds</p>
<p id="message"></p>
</div>

</main>


<script>

const events = __EVENT_DATA__;

let index = 0;
let timer = null;

function render() {
    const e = events[index];

    document.getElementById("event").textContent = e.number + " / " + events.length;
    document.getElementById("time").textContent = e.time.toFixed(3);
    document.getElementById("message").textContent = e.message;

    const philosopherArea = document.getElementById("philosophers");
    philosopherArea.innerHTML = "";

    e.states.forEach((state, i) => {
        philosopherArea.innerHTML += `
            <div class="philosopher">
                <h2>P${i}</h2>
                <strong>${state}</strong>
                <p>Meals completed: ${e.meals[i]}</p>
            </div>
        `;
    });

    const forkArea = document.getElementById("forks");
    forkArea.innerHTML = "";

    e.forkOwners.forEach((owner, i) => {
        const status = owner === -1 ? "Available" : "Owned by P" + owner;

        forkArea.innerHTML += `
            <div class="fork">
                <strong>F${i}</strong>
                <br><br>
                ${status}
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
    }, 800);
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

        Path output = folder.resolve("dining_philosophers_monitor.html");
        Files.writeString(output, html, StandardCharsets.UTF_8);

        System.out.println("\nHTML generated: " + output.toAbsolutePath());
    }


    // -----------------------------------------------------
    // Main
    // -----------------------------------------------------

    public static void main(String[] args) throws Exception {
        Arrays.fill(philosopherStates, "Thinking");

        recordEvent("Dining Philosophers monitor simulation started");

        List<Thread> philosophers = new ArrayList<>();

        for (int i = 0; i < PHILOSOPHER_COUNT; i++) {
            philosophers.add(new Thread(new Philosopher(i), "Philosopher-" + i));
        }

        Collections.shuffle(philosophers);

        for (Thread philosopher : philosophers) {
            philosopher.start();
        }

        for (Thread philosopher : philosophers) {
            philosopher.join();
        }

        recordEvent("All philosophers completed");

        generateHTML();

        System.out.println("\nSimulation completed successfully.");
    }
}