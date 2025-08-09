#!/usr/bin/env python3
import os
import time
import sys
import signal
import serial
import paho.mqtt.client as mqtt

# ---------- Config via env vars ----------
SERIAL_PORT    = os.getenv("SERIAL_PORT", "/dev/ttyHS2")
BAUDRATE       = int(os.getenv("BAUDRATE", "19200"))

MQTT_HOST      = os.getenv("MQTT_HOST", "mqtt")
MQTT_PORT      = int(os.getenv("MQTT_PORT", "1883"))
MQTT_USER      = os.getenv("MQTT_USER", "")
MQTT_PASS      = os.getenv("MQTT_PASS", "")
TOPIC_PREFIX   = os.getenv("TOPIC_PREFIX", "victron/mppt")
AVAIL_TOPIC    = os.getenv("AVAIL_TOPIC", "victron/status")
PUBLISH_RETAIN = os.getenv("RETAIN", "true").lower() in ("1", "true", "yes")

# Keys die we niet willen publiceren (bevatten # of *)
FORBIDDEN_KEY_CHARS = ("#", "*")

# Watchdogs
FRAME_IDLE_TIMEOUT_S = int(os.getenv("FRAME_IDLE_TIMEOUT_S", "8"))   # reset buffer als er zolang geen regels kwamen
FRAME_MAX_LINES      = int(os.getenv("FRAME_MAX_LINES", "128"))      # bescherm tegen runaway

# ---------- MQTT setup ----------
client = mqtt.Client(client_id=f"victron-uart-{int(time.time())}", clean_session=True)
client.will_set(AVAIL_TOPIC, "offline", retain=True)

if MQTT_USER:
    client.username_pw_set(MQTT_USER, MQTT_PASS)

def mqtt_connect():
    while True:
        try:
            client.connect(MQTT_HOST, MQTT_PORT, 60)
            client.loop_start()
            client.publish(AVAIL_TOPIC, "online", retain=True)
            print(f"MQTT connected to {MQTT_HOST}:{MQTT_PORT}", flush=True)
            return
        except Exception as e:
            print(f"MQTT connect failed: {e}. Retry in 5s", flush=True)
            time.sleep(5)

# ---------- Serial ----------
def open_serial():
    while True:
        try:
            ser = serial.Serial(SERIAL_PORT, BAUDRATE, timeout=1)
            print(f"Serial opened on {SERIAL_PORT} @ {BAUDRATE}", flush=True)
            return ser
        except Exception as e:
            print(f"Serial open failed: {e}. Retry in 5s", flush=True)
            time.sleep(5)

# ---------- Helpers ----------
def parse_kv(line: str):
    # VE.Direct: "KEY\tVALUE"
    if "\t" not in line:
        return None, None
    k, v = line.split("\t", 1)
    return k.strip(), v.strip()

def is_forbidden_key(key: str) -> bool:
    return any(ch in key for ch in FORBIDDEN_KEY_CHARS)

def publish_frame(frame: dict):
    # filter verboden keys en publiceer
    cleaned = {k: v for k, v in frame.items() if k and not is_forbidden_key(k)}
    for k, v in cleaned.items():
        client.publish(f"{TOPIC_PREFIX}/{k}", v, retain=PUBLISH_RETAIN)
    client.publish(f"{TOPIC_PREFIX}/_ts", int(time.time()), retain=PUBLISH_RETAIN)

def graceful_exit(*_):
    try:
        client.publish(AVAIL_TOPIC, "offline", retain=True)
        client.loop_stop()
        client.disconnect()
    except Exception:
        pass
    sys.exit(0)

signal.signal(signal.SIGINT, graceful_exit)
signal.signal(signal.SIGTERM, graceful_exit)

# ---------- Main loop (zonder checksum) ----------
def main():
    mqtt_connect()
    ser = open_serial()

    frame_kv = {}
    last_line_ts = time.time()
    line_count = 0

    while True:
        try:
            raw = ser.readline()
            now = time.time()

            # Idle watchdog: als te lang geen data → reset frame buffer
            if now - last_line_ts > FRAME_IDLE_TIMEOUT_S and frame_kv:
                frame_kv.clear()
                line_count = 0

            if not raw:
                # geen nieuwe regel, gewoon door
                continue

            last_line_ts = now
            try:
                line = raw.decode("utf-8", errors="ignore").strip()
            except Exception:
                continue
            if not line:
                continue

            key, val = parse_kv(line)
            if key is None:
                continue

            # Bewaar key/value (filteren pas bij publish)
            frame_kv[key] = val
            line_count += 1

            # Fallback runaway-protectie
            if line_count > FRAME_MAX_LINES:
                frame_kv.clear()
                line_count = 0
                continue

            # Einde van frame:
            # 1) voorkeur: HSDS gezien (praktisch einde telegram)
            # 2) alternatief: 'Checksum' gezien (maar we valideren hem niet meer)
            if key == "HSDS" or key.lower() == "checksum":
                publish_frame(frame_kv)
                frame_kv.clear()
                line_count = 0

        except serial.SerialException as e:
            print(f"Serial error: {e}. Reopening port in 3s...", flush=True)
            time.sleep(3)
            ser = open_serial()
            frame_kv.clear()
            line_count = 0
        except Exception as e:
            print(f"Loop error: {e}", flush=True)

if __name__ == "__main__":
    main()
