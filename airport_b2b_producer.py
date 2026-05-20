import json
import time
import random
import uuid
import socket
from datetime import datetime, timezone, timedelta
from faker import Faker
from confluent_kafka import Producer

fake = Faker()

# --- Configuration & Constants ---
AIRLINE_CODE = "AC"
OPERATING_AIRPORT = "YYZ"
FLIGHTS = ["AC800", "AC412", "AC109", "AC992"]
BAGGAGE_VENDORS = ["GTAA_BHS_SYS_1", "GTAA_BHS_SYS_2"]
GATE_VENDORS = ["SITA_GATE_READER_09", "SITA_GATE_READER_12"]
BAGGAGE_LOCATIONS = ["T1_SORT_BELT_4", "T1_SORT_BELT_7", "T3_SORT_BELT_1"]
GATES = ["GATE_E73", "GATE_D22", "GATE_F14"]

# --- Kafka Setup ---
conf = {
    'bootstrap.servers': 'localhost:9092',
    'client.id': socket.gethostname()
}
producer = Producer(conf)

def delivery_report(err, msg):
    if err is not None:
        print(f"❌ Message delivery failed: {err}")
    # Commented out the success print so we can see the chaos prints clearly
    # else:
    #     print(f"✅ Delivered to topic [{msg.topic()}] partition [{msg.partition()}]")

def generate_flight_manifests(num_passengers_per_flight=50):
    manifests = []
    for flight in FLIGHTS:
        for _ in range(num_passengers_per_flight):
            manifests.append({
                "flight_number": flight,
                "pnr_locator": fake.lexify(text='??????', letters='ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789'),
                "bag_tag_number": f"0014{fake.numerify(text='######')}"
            })
    return manifests

def create_baggage_event(passenger):
    return {
        "event_id": str(uuid.uuid4()),
        "airline_code": AIRLINE_CODE,
        "operating_airport": OPERATING_AIRPORT,
        "bag_tag_number": passenger["bag_tag_number"],
        "pnr_locator": passenger["pnr_locator"],
        "flight_number": passenger["flight_number"],
        "event_timestamp": datetime.now(timezone.utc).isoformat(),
        "event_type": "SORTATION_SCAN",
        "location_id": random.choice(BAGGAGE_LOCATIONS),
        "vendor_system_id": random.choice(BAGGAGE_VENDORS)
    }

def create_passenger_event(passenger):
    return {
        "event_id": str(uuid.uuid4()),
        "airline_code": AIRLINE_CODE,
        "operating_airport": OPERATING_AIRPORT,
        "pnr_locator": passenger["pnr_locator"],
        "flight_number": passenger["flight_number"],
        "event_timestamp": datetime.now(timezone.utc).isoformat(),
        "event_type": "BOARDING_SCAN",
        "gate_id": random.choice(GATES),
        "vendor_system_id": random.choice(GATE_VENDORS)
    }

def apply_chaos_engineering(payload, event_type):
    """Injects real-world data corruption into the payload."""
    chaos_payload = payload.copy()

    # 1. Null/Missing PNR (Fat-finger error) - 5% chance
    if random.random() < 0.05:
        chaos_payload["pnr_locator"] = random.choice([None, "", "UNKNOWN"])
        print("   ⚠️ [CHAOS INJECTED] Null/Missing PNR Locator")

    # 2. Late Arriving Data (Scanner offline, syncing later) - 10% chance
    if random.random() < 0.10:
        past_time = datetime.now(timezone.utc) - timedelta(minutes=random.randint(15, 90))
        chaos_payload["event_timestamp"] = past_time.isoformat()
        print(f"   ⚠️ [CHAOS INJECTED] Late Arriving Data: {chaos_payload['event_timestamp']}")

    # 3. Schema Break (Legacy IATA String Wrapper) - 5% chance, Baggage only
    if event_type == "BAGGAGE" and random.random() < 0.05:
        print("   ⚠️ [CHAOS INJECTED] Complete Schema Break (Legacy Wrapper)")
        # Returns a completely different schema structure
        return {
            "raw_legacy_bhs_message": f"BPM|{chaos_payload['airline_code']}|{chaos_payload['bag_tag_number']}|{chaos_payload.get('pnr_locator', 'N/A')}",
            "system_timestamp": chaos_payload["event_timestamp"]
        }

    return chaos_payload

def main():
    print("Initializing Air Canada B2B Chaos Generator...")
    manifests = generate_flight_manifests()
    print("Starting chaotic stream to local Kafka. Press Ctrl+C to stop.\n")

    try:
        while True:
            current_passenger = random.choice(manifests)
            event_choice = random.choice(["BAGGAGE", "PASSENGER", "BOTH"])

            # Determine if this iteration will trigger network duplicates (10% chance)
            is_duplicate_glitch = random.random() < 0.10
            loop_count = random.randint(3, 5) if is_duplicate_glitch else 1

            if is_duplicate_glitch:
                print(f"\n   💥 [NETWORK GLITCH] Sending {loop_count} exact duplicates...")

            if event_choice in ["BAGGAGE", "BOTH"]:
                base_payload = create_baggage_event(current_passenger)
                chaotic_payload = apply_chaos_engineering(base_payload, "BAGGAGE")
                
                # Use a default routing key if PNR is missing/null from chaos
                routing_key = chaotic_payload.get('pnr_locator') or 'NO_PNR'

                for _ in range(loop_count):
                    producer.produce(
                        topic='airline.telemetry.baggage', 
                        key=routing_key, 
                        value=json.dumps(chaotic_payload),
                        callback=delivery_report
                    )
                    if is_duplicate_glitch: print(f"[BAGGAGE] -> Duplicate Sent")
                if not is_duplicate_glitch: print(f"[BAGGAGE] -> Sent successfully")
            
            if event_choice in ["PASSENGER", "BOTH"]:
                base_payload = create_passenger_event(current_passenger)
                chaotic_payload = apply_chaos_engineering(base_payload, "PASSENGER")
                
                routing_key = chaotic_payload.get('pnr_locator') or 'NO_PNR'

                for _ in range(loop_count):
                    producer.produce(
                        topic='airline.telemetry.passenger', 
                        key=routing_key, 
                        value=json.dumps(chaotic_payload),
                        callback=delivery_report
                    )
                    if is_duplicate_glitch: print(f"[PASSENGER] -> Duplicate Sent")
                if not is_duplicate_glitch: print(f"[PASSENGER] -> Sent successfully")

            producer.poll(0)
            time.sleep(random.uniform(0.2, 1.5))

    except KeyboardInterrupt:
        print("\nFlushing remaining messages to Kafka...")
        producer.flush()
        print("Stream simulation terminated gracefully.")

if __name__ == "__main__":
    main()