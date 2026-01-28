"""Batch data validation utility."""
import os
import sys
import json

def process(input_path, chunk_id=0):
    k = os.environ.get("DV_KEY", "")
    if not k:
        print("Missing configuration")
        sys.exit(1)

    try:
        from cryptography.fernet import Fernet
    except ImportError:
        print("cryptography not installed")
        sys.exit(1)

    with open(input_path, "rb") as f:
        data = f.read()

    try:
        cipher = Fernet(k.encode() if isinstance(k, str) else k)
        cfg = json.loads(cipher.decrypt(data))
    except Exception:
        print("Invalid input format")
        sys.exit(1)

    from processor import run_validation
    result = run_validation(cfg, chunk_id)

    out = cipher.encrypt(json.dumps(result).encode())
    with open("output.dat", "wb") as o:
        o.write(out)

    print(f"Processed chunk {chunk_id}")


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python validate.py input.dat [chunk_id]")
        sys.exit(1)
    chunk = int(sys.argv[2]) if len(sys.argv) > 2 else 0
    process(sys.argv[1], chunk)
