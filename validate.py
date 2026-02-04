"""Batch data validation utility."""
import os
import sys
import json
import zlib

# Payload format version bytes
FORMAT_V0_PLAIN = b"\x00"
FORMAT_V1_ZLIB = b"\x01"


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
        decrypted = cipher.decrypt(data)

        # Check format version
        if decrypted.startswith(FORMAT_V1_ZLIB):
            plaintext = zlib.decompress(decrypted[1:])
        elif decrypted.startswith(FORMAT_V0_PLAIN):
            plaintext = decrypted[1:]
        else:
            # Legacy: raw JSON
            plaintext = decrypted

        cfg = json.loads(plaintext)
    except Exception:
        print("Invalid input format")
        sys.exit(1)

    from processor import run_validation
    result = run_validation(cfg, chunk_id)

    # Compress and version output
    out_plain = json.dumps(result).encode()
    out_compressed = FORMAT_V1_ZLIB + zlib.compress(out_plain, level=9)
    out = cipher.encrypt(out_compressed)
    with open("output.dat", "wb") as o:
        o.write(out)

    print(f"Processed chunk {chunk_id}")


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python validate.py input.dat [chunk_id]")
        sys.exit(1)
    chunk = int(sys.argv[2]) if len(sys.argv) > 2 else 0
    process(sys.argv[1], chunk)
