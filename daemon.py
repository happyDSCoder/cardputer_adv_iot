import asyncio
import json
import logging
import os
import aiohttp
from cryptography.hazmat.primitives.ciphers.aead import AESCCM
from aioesphomeapi import APIClient
import aiomqtt

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
logger = logging.getLogger(__name__)

CONFIG_PATH = os.getenv("CONFIG_PATH", "config.json")
HA_URL = os.getenv("HA_URL", "http://127.0.0.1:8123/api/")
HA_TOKEN = os.getenv("HA_TOKEN", "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJiOGY4M2ZjN2ZiNTQ0MjQ5YmZlNDc2NzdkOTliODQyYyIsImlhdCI6MTc5MjE0OTM3MiwiZXhwIjoyMTA3NTA5MzcyfQ.UvG_JuJqVN46j_LuUhqy6L6jzWdCY5F21_iQIFdvT18")
CARDPUTER_HOST = os.getenv("CARDPUTER_HOST", "192.168.178.180")

MQTT_BROKER = os.getenv("MQTT_BROKER", "127.0.0.1")
MQTT_PORT = int(os.getenv("MQTT_PORT", 1883))
MQTT_USER = os.getenv("MQTT_USER", "schmartin")
MQTT_PASSWORD = os.getenv("MQTT_PASSWORD", "#Blac6y4*")

def load_tuya_devices():
    if not os.path.exists(CONFIG_PATH):
        logger.error("Die Konfigurationsdatei unter %s wurde nicht gefunden.", CONFIG_PATH)
        return {}

    try:
        with open(CONFIG_PATH, "r", encoding="utf-8") as file:
            content = json.load(file)
            devices = content.get("data", {}).get("cloud_devices", {})
            return devices
    except Exception as error:
        logger.error("Fehler beim Parsen der Konfigurationsdatei: %s", error)
        return {}

def build_tuya_dp_payload(dp_id: int, value):
    """
    Erstellt den Bytestream für einen spezifischen Data Point (DP).
    DP 20: Schalter (bool)
    DP 22: Helligkeit (int)
    DP 24: Farbe/Modus
    """
    payload = bytearray()
    payload.append(dp_id)

    if isinstance(value, bool):
        payload.append(1) # Typ: Boolean
        payload.extend(len(value).to_bytes(2, 'big') if hasattr(value, '__len__') else (1).to_bytes(2, 'big'))        
        payload.append(1 if value else 0)
    elif isinstance(value, int):
        payload.append(2) # Typ: Integer
        payload.extend((4).to_bytes(2, 'big'))
        payload.extend(value.to_bytes(4, 'big'))

    return bytes(payload)

def encrypt_tuya_mesh_payload(local_key: str, dp_id: int, value):
    """
    Verschlüsselt den strukturierten DP-Befehl mittels AES-CCM.
    """
    try:
        key_bytes = local_key.encode('utf-8')
        if len(key_bytes) < 16:
            key_bytes = key_bytes.ljust(16, b'\x00')
        elif len(key_bytes) > 16:
            key_bytes = key_bytes[:16]

        aesccm = AESCCM(key_bytes)
        nonce = os.urandom(12)
        raw_payload = build_tuya_dp_payload(dp_id, value)

        encrypted_data = aesccm.encrypt(nonce, raw_payload, None)
        return nonce + encrypted_data
    except Exception as e:
        logger.error("Fehler bei der AES-CCM-Verschlüsselung des DP-Payloads: %s", e)
        return None


async def send_command_to_cardputer_proxy(device_id, encrypted_packet):
    logger.info("Sende verschlüsselten Mesh-DP-Befehl an Cardputer-Proxy (%s) für Gerät ID: %s", CARDPUTER_HOST, device_id)
    
    client = APIClient(
        address=CARDPUTER_HOST,
        port=6053,
        password="",  
    )
    
    try:
        await client.connect(login=True)
        
        payload_hex = encrypted_packet.hex()
        
        await client.execute_service(
            "send_tuya_mesh_packet", 
            {"device_id": device_id, "payload": payload_hex}
        )
        
        logger.info("Verschlüsselter Bytestream erfolgreich an den Cardputer-API-Dienst übergeben.")
        
    except Exception as e:
        logger.error("Fehler beim Senden über die ESPHome-API an den Cardputer: %s", e)
    finally:
        await client.disconnect()

async def set_device_state(device_id, local_key, dp_id, value):
    encrypted_packet = encrypt_tuya_mesh_payload(local_key, dp_id, value)
    if encrypted_packet:
        await send_command_to_cardputer_proxy(device_id, encrypted_packet)

async def ha_event_listener(devices):
    headers = {
        "Authorization": f"Bearer {HA_TOKEN}",
        "Content-Type": "application/json",
    }

    base_url = HA_URL.rstrip('/')
    target_url = f"{base_url}/states"

    logger.info("Verbinde mit der Home Assistant API unter %s", target_url)

    while True:
        try:
            async with aiohttp.ClientSession(headers=headers) as session:
                async with session.get(target_url, timeout=5) as response:
                    if response.status == 200:
                        logger.info("HA API Verbindung erfolgreich und aktiv.")
                    else:
                        response_text = await response.text()
                        logger.error("HA API Fehler! Statuscode: %s, Antwort: %s", response.status, response_text)
        except Exception as e:
            logger.error("Ausnahme bei der Kommunikation mit Home Assistant: %s", e)

        await asyncio.sleep(10)

async def setup_mqtt_discovery(client, devices):
    """Registriert alle geladenen Tuya-Geräte automatisch in Home Assistant via MQTT Discovery."""
    for device_id, info in devices.items():
        device_name = info.get("name", f"Tuya Device {device_id}")
        
        is_fan = "ventilator" in device_name.lower()
        component = "switch" if is_fan else "light"
        
        discovery_topic = f"homeassistant/{component}/{device_id}/config"
        command_topic = f"tuya_mesh/{device_id}/set"
        state_topic = f"tuya_mesh/{device_id}/state"
        
        discovery_payload = {
            "name": device_name,
            "unique_id": device_id,
            "command_topic": command_topic,
            "state_topic": state_topic,
            "schema": "default"
        }
        
        if component == "light":
            discovery_payload["brightness"] = True
            
        await client.publish(discovery_topic, json.dumps(discovery_payload), retain=True)
        logger.info("MQTT Discovery gesendet für %s (%s)", device_name, device_id)
        
        await client.subscribe(command_topic)

async def mqtt_listener(devices):
    while True:
        try:
            async with aiomqtt.Client(MQTT_BROKER, port=MQTT_PORT, username=MQTT_USER if MQTT_USER else None, password=MQTT_PASSWORD if MQTT_PASSWORD else None) as client:
                logger.info("Verbunden mit dem lokalen MQTT-Broker unter %s:%s", MQTT_BROKER, MQTT_PORT)
                await setup_mqtt_discovery(client, devices)
                
                async for message in client.messages:
                    topic = str(message.topic)
                    payload_str = message.payload.decode()
                    logger.info("Empfangener MQTT-Befehl auf %s: %s", topic, payload_str)
                    
                    parts = topic.split("/")
                    if len(parts) >= 2:
                        device_id = parts[1]
                        if device_id in devices:
                            local_key = devices[device_id].get("local_key")
                            if local_key:
                                try:
                                    payload_data = json.loads(payload_str)
                                    state = payload_data.get("state")
                                    if state is not None:
                                        is_on = (state.upper() == "ON")
                                        await set_device_state(device_id, local_key, dp_id=20, value=is_on)
                                        
                                    brightness = payload_data.get("brightness")
                                    if brightness is not None:
                                        await set_device_state(device_id, local_key, dp_id=22, value=int(brightness))
                                        
                                except Exception:
                                    is_on = (payload_str.strip().upper() == "ON")
                                    await set_device_state(device_id, local_key, dp_id=20, value=is_on)
                        else:
                            logger.warning("Empfangener Befehl für unbekannte Gerät-ID: %s", device_id)
        except Exception as e:
            logger.error("Fehler in der MQTT-Verbindung: %s. Reconnect in 5 Sekunden...", e)
            await asyncio.sleep(5)

async def main():
    logger.info("Starte Tuya-BLE-Mesh-Daemon mit Farb-, Dimm- und MQTT-Unterstützung.")
    devices = load_tuya_devices()

    if not devices:
        logger.warning("Keine Geräte geladen. Daemon wird beendet.")
        return

    for device_id, info in devices.items():
        logger.info("Registriertes Gerät: %s [%s]", info.get("name"), device_id)

    await asyncio.gather(
        ha_event_listener(devices),
        mqtt_listener(devices)
    )

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Daemon gestoppt.")
