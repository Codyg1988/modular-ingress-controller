#!/usr/bin/env python3
"""
Modular Ingress Controller - Standard Synchronous Version with Server Connection
Validates states and automatically pipes payloads to the Stream Broker Server port.
"""

import json
import urllib.request
from typing import Dict, Any

class StandaloneFiltrationSystem:
    def __init__(self):
        # Operational Configuration Thresholds
        self.max_payload_length = 50000
        # Target network port for the stream broker server
        self.target_routing_url = "http://127.0.0.1:8000/api/v1/telemetry/submit"

    def filter_incoming_payload(self, raw_data: Dict[str, Any]) -> Dict[str, Any]:
        """Intercepts, cleans, and evaluates incoming data vectors."""
        packet_id = raw_data.get("id", "UNKNOWN_ID")
        content = raw_data.get("content", "")

        # Structural Anomaly Isolation
        if not content or str(content).isspace():
            return {"id": packet_id, "status": "VOID_PURGE", "payload": None}
            
        if len(str(content)) > self.max_payload_length:
            return {"id": packet_id, "status": "OVERWHELM_STREAM", "payload": None}

        # Clean Vector Formed
        validated_payload = {
            "id": packet_id,
            "content": str(content).strip()
        }
        
        result = {"id": packet_id, "status": "CLEAN_VALIDATED", "payload": validated_payload}
        
        # Automatically connect and dispatch to the server port
        self.dispatch_to_server(result)
        
        return result

    def dispatch_to_server(self, result: Dict[str, Any]):
        """Opens a direct network socket and pipes data to the server port."""
        try:
            # Match the data contract the server expects
            payload_data = json.dumps({
                "id": result["payload"]["id"],
                "status": result["status"],
                "content": result["payload"]["content"]
            }).encode("utf-8")
            
            req = urllib.request.Request(
                self.target_routing_url, 
                data=payload_data, 
                headers={"Content-Type": "application/json"}
            )
            
            with urllib.request.urlopen(req) as response:
                if response.status == 200:
                    print("🟢 SUCCESS: Ingress Controller connected to server port 8000 and delivered payload!")
        except Exception as e:
            print(f"❌ Connection Failed: Server port 8000 is closed or unreachable. Error: {e}")
