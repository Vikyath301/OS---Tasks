import threading
import time
import numpy as np
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import matplotlib.animation as animation

ROWS_A = 50
COLS_A_ROWS_B = 50
COLS_B = 50

MAX_CONCURRENT_THREADS = 8

K_DELAY = 0.004
CELL_DELAY = 0.01
FPS = 20
TOTAL_SECONDS_CAP = 120

np.random.seed(7)
A = np.random.randint(0, 10, size=(ROWS_A, COLS_A_ROWS_B)).astype(np.float64)
B = np.random.randint(0, 10, size=(COLS_A_ROWS_B, COLS_B)).astype(np.float64)
C = np.zeros((ROWS_A, COLS_B), dtype=np.float64)
completed_mask = np.zeros((ROWS_A, COLS_B), dtype=bool)

state_lock = threading.Lock()
sem = threading.Semaphore(MAX_CONCURRENT_THREADS)

active = {}

progress = {"cells_done": 0, "rows_done": 0, "active_threads": 0, "start_time": None}
TOTAL_CELLS = ROWS_A * COLS_B


def compute_row(row_idx: int):
    sem.acquire()
    with state_lock:
        progress["active_threads"] += 1

    for col in range(COLS_B):
        acc = 0.0
        for k in range(COLS_A_ROWS_B):
            acc += A[row_idx, k] * B[k, col]
            with state_lock:
                active[row_idx] = (col, k)
            if K_DELAY:
                time.sleep(K_DELAY)

        with state_lock:
            C[row_idx, col] = acc
            completed_mask[row_idx, col] = True
            progress["cells_done"] += 1

        if CELL_DELAY:
            time.sleep(CELL_DELAY)

    with state_lock:
        active.pop(row_idx, None)
        progress["rows_done"] += 1
        progress["active_threads"] -= 1
    sem.release()


threads = [threading.Thread(target=compute_row, args=(r,), daemon=True)
           for r in range(ROWS_A)]

progress["start_time"] = time.time()
for t in threads:
    t.start()

print(f"Launched {len(threads)} threads (one per row of A). "
      f"A semaphore caps concurrent execution at {MAX_CONCURRENT_THREADS} threads at a time.")

plt.style.use("dark_background")
fig, (ax_a, ax_b, ax_c) = plt.subplots(1, 3, figsize=(13, 4.8))
fig.patch.set_facecolor("#0b0b12")

CMAP_A, CMAP_B, CMAP_C = "cool", "YlOrBr", "turbo"

im_a = ax_a.imshow(A, cmap=CMAP_A, aspect="auto")
ax_a.set_title(f"Matrix A\n{ROWS_A} x {COLS_A_ROWS_B}", fontsize=10)
ax_a.set_xticks([]); ax_a.set_yticks([])

im_b = ax_b.imshow(B, cmap=CMAP_B, aspect="auto")
ax_b.set_title(f"Matrix B\n{COLS_A_ROWS_B} x {COLS_B}", fontsize=10)
ax_b.set_xticks([]); ax_b.set_yticks([])

display_c = np.ma.masked_array(np.zeros_like(C), mask=~completed_mask)
im_c = ax_c.imshow(display_c, cmap=CMAP_C, aspect="auto",
                    vmin=0, vmax=(COLS_A_ROWS_B * 81))
title_c = ax_c.set_title(f"Result C - 0 / {TOTAL_CELLS} cells", fontsize=10)
ax_c.set_xticks([]); ax_c.set_yticks([])

status_text = fig.text(0.5, 0.02, "", ha="center", va="center",
                        fontsize=9, color="#cfd8ff", family="monospace")
fig.suptitle("Threaded Matrix Multiplication - live sweep (A: left->right, B: top->down)",
              fontsize=12, color="#e8e8ff")
fig.tight_layout(rect=[0, 0.06, 1, 0.93])

row_lines, row_dots, col_lines, col_dots = [], [], [], []
for _ in range(MAX_CONCURRENT_THREADS):
    rl, = ax_a.plot([], [], color="white", linewidth=1.6, alpha=0.9)
    rd, = ax_a.plot([], [], marker="o", markersize=5, color="white",
                     markeredgecolor="black")
    cl, = ax_b.plot([], [], color="white", linewidth=1.6, alpha=0.9)
    cd, = ax_b.plot([], [], marker="o", markersize=5, color="white",
                     markeredgecolor="black")
    row_lines.append(rl); row_dots.append(rd)
    col_lines.append(cl); col_dots.append(cd)


def update(frame):
    with state_lock:
        cells_done = progress["cells_done"]
        rows_done = progress["rows_done"]
        active_count = progress["active_threads"]
        c_snapshot = C.copy()
        mask_snapshot = completed_mask.copy()
        active_snapshot = list(active.items())

    disp = np.ma.masked_array(c_snapshot, mask=~mask_snapshot)
    im_c.set_data(disp)
    title_c.set_text(f"Result C - {cells_done} / {TOTAL_CELLS} cells")

    for i in range(MAX_CONCURRENT_THREADS):
        if i < len(active_snapshot):
            row, (col, k) = active_snapshot[i]
            row_lines[i].set_data([0, k], [row, row])
            row_dots[i].set_data([k], [row])
            col_lines[i].set_data([col, col], [0, k])
            col_dots[i].set_data([col], [k])
        else:
            row_lines[i].set_data([], [])
            row_dots[i].set_data([], [])
            col_lines[i].set_data([], [])
            col_dots[i].set_data([], [])

    elapsed = max(time.time() - progress["start_time"], 1e-6)
    rate = cells_done / elapsed
    pct = 100.0 * cells_done / TOTAL_CELLS
    status_text.set_text(
        f"Row {rows_done}/{ROWS_A} completed   |   "
        f"{cells_done}/{TOTAL_CELLS} cells   |   "
        f"{pct:5.1f}%   |   {rate:6.1f} cells/sec   |   "
        f"{active_count} active / {MAX_CONCURRENT_THREADS} lanes   |   "
        f"{ROWS_A} threads total"
    )
    return [im_c, title_c, status_text] + row_lines + row_dots + col_lines + col_dots


max_frames = FPS * TOTAL_SECONDS_CAP
frame_idx = 0


def frame_gen():
    global frame_idx
    while frame_idx < max_frames:
        with state_lock:
            done = progress["rows_done"] >= ROWS_A
        yield frame_idx
        frame_idx += 1
        if done:
            for _ in range(FPS):
                yield frame_idx
                frame_idx += 1
            return


ani = animation.FuncAnimation(
    fig, update, frames=frame_gen, interval=1000 / FPS,
    blit=False, repeat=False, cache_frame_data=False
)

try:
    writer = animation.FFMpegWriter(fps=FPS, bitrate=1800)
    ani.save("threaded_matmul.mp4", writer=writer, dpi=120)
    print("Saved threaded_matmul.mp4")
except Exception as e:
    print("FFmpeg writer failed, falling back to GIF:", e)
    writer = animation.PillowWriter(fps=FPS)
    ani.save("threaded_matmul.gif", writer=writer, dpi=100)
    print("Saved threaded_matmul.gif")

for t in threads:
    t.join()

expected = A @ B
assert np.allclose(C, expected), "Mismatch between threaded result and numpy!"
print(f"All {len(threads)} threads finished. "
      f"Result verified correct against numpy for a "
      f"{ROWS_A}x{COLS_A_ROWS_B} @ {COLS_A_ROWS_B}x{COLS_B} multiplication.")