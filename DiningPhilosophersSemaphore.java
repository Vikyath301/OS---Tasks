import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.Semaphore;
import java.util.concurrent.ThreadLocalRandom;

public class DiningPhilosophersSemaphore {

    static final int PHILOSOPHER_COUNT = 4;
    static final int MEALS_PER_PHILOSOPHER = 3;

    /*
     * One binary semaphore for each fork.
     */
    static final Semaphore[] forks = new Semaphore[PHILOSOPHER_COUNT];

    /*
     * At most 3 philosophers may attempt
     * to acquire forks at once.
     *
     * This prevents circular-wait deadlock.
     */
    static final Semaphore room = new Semaphore(PHILOSOPHER_COUNT - 1, true);

    static final Object stateLock = new Object();

    static final String[] philosopherStates = new String[PHILOSOPHER_COUNT];

    /*
     * -1 means fork is available.
     * Otherwise the value is philosopher ID.
     */
    static final int[] forkOwners = new int[PHILOSOPHER_COUNT];

    static final int[] meals = new int[PHILOSOPHER_COUNT];

    static final List<Event> events = new ArrayList<>();

    static final long startTime = System.nanoTime();


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


    static void randomSleep(int min, int max) {
        try {
            Thread.sleep(ThreadLocalRandom.current().nextInt(min, max + 1));
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
                    forkOwners,
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
        private final int leftFork;
        private final int rightFork;

        Philosopher(int id) {
            this.id = id;
            this.leftFork = id;
            this.rightFork = (id + 1) % PHILOSOPHER_COUNT;
        }

        @Override
        public void run() {
            try {
                for (int meal = 1; meal <= MEALS_PER_PHILOSOPHER; meal++) {
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
                     * Deadlock prevention.
                     */
                    room.acquire();

                    synchronized (stateLock) {
                        philosopherStates[id] = "Acquiring Forks";
                    }
                    recordEvent("P" + id + " is trying to acquire F" + leftFork + " and F" + rightFork);

                    // Acquire left fork
                    forks[leftFork].acquire();

                    synchronized (stateLock) {
                        forkOwners[leftFork] = id;
                    }
                    recordEvent("P" + id + " acquired F" + leftFork);

                    // Acquire right fork
                    forks[rightFork].acquire();

                    synchronized (stateLock) {
                        forkOwners[rightFork] = id;
                        philosopherStates[id] = "Eating";
                    }
                    recordEvent("P" + id + " acquired F" + rightFork + " and started eating");

                    randomSleep(550, 1000);

                    synchronized (stateLock) {
                        meals[id]++;
                    }
                    recordEvent("P" + id + " completed meal " + meals[id]);

                    synchronized (stateLock) {
                        philosopherStates[id] = "Releasing Forks";
                    }
                    recordEvent("P" + id + " is releasing forks");

                    /*
                     * Release right fork.
                     */
                    synchronized (stateLock) {
                        forkOwners[rightFork] = -1;
                    }
                    forks[rightFork].release();
                    recordEvent("P" + id + " released F" + rightFork);

                    /*
                     * Release left fork.
                     */
                    synchronized (stateLock) {
                        forkOwners[leftFork] = -1;
                    }
                    forks[leftFork].release();
                    recordEvent("P" + id + " released F" + leftFork);

                    /*
                     * Philosopher leaves the room.
                     */
                    room.release();
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
<title>Dining Philosophers Semaphore</title>

<style>
body {
    font-family: Arial, sans-serif;
    background: #f3f4f6;
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
    border-radius: 20px;
    padding: 25px;
}

.philosophers,
.forks {
    display: grid;
    grid-template-columns: repeat(4, 1fr);
    gap: 15px;
}

.philosopher {
    background: #eef2ff;
    padding: 20px;
    border-radius: 12px;
    text-align: center;
}

.fork {
    background: #ffffff;
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

<h1>Dining Philosophers - Semaphore Simulation</h1>

<div class="table">
<h2>Philosophers</h2>
<div class="philosophers" id="philosophers"></div>

<h2>Fork Ownership</h2>
<div class="forks" id="forks"></div>
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

    const philosophers = document.getElementById("philosophers");
    philosophers.innerHTML = "";

    e.states.forEach((state, i) => {
        philosophers.innerHTML += `
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

        Path output = folder.resolve("dining_philosophers_semaphore.html");
        Files.writeString(output, html, StandardCharsets.UTF_8);

        System.out.println("\nHTML generated: " + output.toAbsolutePath());
    }


    // -----------------------------------------------------
    // Main
    // -----------------------------------------------------

    public static void main(String[] args) throws Exception {
        Arrays.fill(forkOwners, -1);
        Arrays.fill(philosopherStates, "Thinking");

        for (int i = 0; i < PHILOSOPHER_COUNT; i++) {
            forks[i] = new Semaphore(1, true);
        }

        recordEvent("Dining Philosophers semaphore simulation started");

        List<Thread> threads = new ArrayList<>();

        for (int i = 0; i < PHILOSOPHER_COUNT; i++) {
            threads.add(new Thread(new Philosopher(i), "Philosopher-" + i));
        }

        Collections.shuffle(threads);

        for (Thread thread : threads) {
            thread.start();
        }

        for (Thread thread : threads) {
            thread.join();
        }

        recordEvent("All philosophers completed");

        generateHTML();

        System.out.println("\nSimulation completed successfully.");
    }
}