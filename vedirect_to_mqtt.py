#!/usr/bin/env python3
import os
import time
import sys
import signal
import serial
import paho.mqtt.client as mqtt

# ---------- Config via env vars ----------
SERIAL_PORT   = os.getenv("SERIAL_PORT", "/dev/ttyHS2")
BAUDRATE      = int(os.getenv("BAUDRATE", "19200"))

MQTT_HOST     = os.getenv("MQTT_HOST", "mqtt")
MQTT_PORT     = int(os.getenv("MQTT_PORT", "1883"))
MQTT_USER     = os.getenv("MQTT_USER", "")
MQTT_PASS     = os.getenv("MQTT_PASS", "")
TOPIC_PREFIX  = os.getenv("TOPIC_PREFIX", "victron/mppt")
AVAIL_TOPIC   = os.getenv("AVAIL_TOPIC", "victron/status")
PUBLISH_RETAIN = os.getenv("RETAIN", "true").lower() in ("1","true","yes")

# keys met deze tekens worden verwijderd
FORBIDDEN_KEY_CHARS = ("#", "*")

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
    # publiceer alle (gefilterde) key/values
    for k, v in frame.items():
        topic = f"{TOPIC_PREFIX}/{k}"
        client.publish(topic, v, retain=PUBLISH_RETAIN)
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

# ---------- VE.Direct loop met checksum ----------
def main():
    mqtt_connect()
    ser = open_serial()

    # Buffer voor huidig frame
    frame_bytes = bytearray()
    frame_kv = {}

    while True:
        try:
            raw = ser.readline()
            if not raw:
                continue

            # Accumuleer altijd de *ruwe bytes* (incl. newline)
            frame_bytes.extend(raw)

            # Parse leesbare lijn
            try:
                line = raw.decode("utf-8", errors="ignore").strip()
            except Exception:
                # kan niet decoderen → laat maar zitten (bytes zitten al in frame_bytes)
                continue
            if not line:
                continue

            key, val = parse_kv(line)
            if key is None:
                continue

            # Einde van een VE.Direct frame: 'Checksum'
            if key.lower() == "checksum":
                # LRC: Som van ALLE bytes in het frame (inclusief deze regel) moet 0 mod 256 zijn
                lrc_ok = (sum(frame_bytes) & 0xFF) == 0
                if not lrc_ok:
                    # Ongeldig frame → weggooien
                    # print(f"Invalid checksum (sum={sum(frame_bytes)&0xFF}), dropping frame")
                    frame_bytes.clear()
                    frame_kv.clear()
                    continue

                # Geldig frame → filter keys met '#' of '*'
                cleaned = {k: v for k, v in frame_kv.items() if not is_forbidden_key(k)}

                # Publish
                publish_frame(cleaned)

                # Reset buffers voor volgend frame
                frame_bytes.clear()
                frame_kv.clear()
                continue

            # Normale key → opslaan (maar nog niet publiceren)
            # (We slaan ook 'SER#' etc. op; filtering gebeurt pas bij publish)
            frame_kv[key] = val

        except serial.SerialException as e:
            print(f"Serial error: {e}. Reopening port in 3s...", flush=True)
            time.sleep(3)
            ser = open_serial()
            # buffers leegmaken; we zitten midden in een frame dat we niet kunnen vervolgen
            frame_bytes.clear()
            frame_kv.clear()
        except Exception as e:
            # log en ga door
            print(f"Loop error: {e}", flush=True)

if __name__ == "__main__":
    main()
