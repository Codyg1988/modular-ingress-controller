#!/usr/bin/env python3
"""
Standalone Resilience Simulation for Ingress Controller
Validates the Circuit Breaker and Retry states entirely in memory.
"""

import asyncio
import sys
from pathlib import Path

# Force Python to look at your Desktop directory for imports
SCRIPT_DIR = Path(__file__).resolve().parent
if str(SCRIPT_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPT_DIR))

from modular_ingress_controller import StandaloneFiltrationSystem

class MockNetworkSession:
    """Simulates a totally dead downstream network connection"""
    def __init__(self):
        self.status = 500
    async def post(self, url, json):
        # Force a network drop error to test the controller's defense mechanics
        raise Exception("Connection refused by target host (Simulated Outage)")
    async def __aenter__(self): return self
    async def __aexit__(self, exc_type, exc_val, exc_tb): pass

async def run_resilience_test():
    print("🎬 Initializing Ingress Gate Resilience Simulation...")
    ingress = StandaloneFiltrationSystem()
    mock_session = MockNetworkSession()
    
    raw_packet = {"id": "tx_resilient_101", "content": "Enterprise data payload."}
    
    # 1. Run through normal filtration
    validation = await ingress.filter_incoming_payload(raw_packet)
    
    if validation["status"] == "CLEAN_VALIDATED":
        print("\n--- 🔄 TRIGGERING RUN 1: Simulated Downstream Outage ---")
        print("Expectation: Controller should retry with increasing delays, then TRIP the circuit.")
        await ingress.execute_resilient_route(mock_session, validation["payload"])
        
        print("\n--- 🚨 TRIGGERING RUN 2: Immediate Consecutive Request ---")
        print("Expectation: Controller should instantly BLOCK the network call to save CPU/RAM.")
        immediate_retry = await ingress.filter_incoming_payload(raw_packet)
        print(f"Result Status: {immediate_retry['status']}")
        if "reason" in immediate_retry:
            print(f"Reason: {immediate_retry['reason']}")

if __name__ == "__main__":
    asyncio.run(run_resilience_test())
