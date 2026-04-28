import os
import shutil
import logging
import socket

LOCAL_DIRECT_RUNNER = "apache_beam.runners.direct.direct_runner.BundleBasedDirectRunner"

def setup_logging():
    """Configure logging for both local and cloud execution."""
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s [%(levelname)s] %(message)s',
        datefmt='%H:%M:%S'
    )

def is_running_on_gcp():
    """Check if the script is running on Google Cloud hardware."""
    try:
        socket.getaddrinfo('metadata.google.internal', 80)
        return True
    except socket.gaierror:
        return False

def normalize_beam_args(beam_args, input_source):
    """Detect runner automatically: GCP environment -> Dataflow, Local -> Direct."""
    normalized = []
    skip_next = False
    has_runner = False

    for i, arg in enumerate(beam_args):
        if skip_next:
            skip_next = False
            continue
        
        if arg == "--runner" and i + 1 < len(beam_args):
            runner = beam_args[i + 1]
            normalized.extend([arg, LOCAL_DIRECT_RUNNER if runner == "DirectRunner" else runner])
            skip_next = True
            has_runner = True
        elif arg.startswith("--runner="):
            runner = arg.split("=", 1)[1]
            normalized.append(f"--runner={LOCAL_DIRECT_RUNNER}" if runner == "DirectRunner" else arg)
            has_runner = True
        else:
            normalized.append(arg)

    if not has_runner:
        if is_running_on_gcp() or input_source.startswith("gs://") or "gcs" in input_source:
            logging.info("🚀 Cloud environment or source detected. Defaulting to DataflowRunner.")
            normalized.extend(["--runner", "DataflowRunner"])
        else:
            logging.info("💻 Local environment detected. Using DirectRunner.")
            normalized.extend(["--runner", LOCAL_DIRECT_RUNNER])
    
    return normalized

def cleanup_temp():
    """Remove Beam temporary directories and .egg-info metadata."""
    root_dir = "."
    for name in os.listdir(root_dir):
        path = os.path.join(root_dir, name)
        # 1. Clean .egg-info from the root folder
        if name.endswith(".egg-info") and os.path.isdir(path):
            try:
                shutil.rmtree(path)
                logging.info(f"🧹 Cleaned up metadata: {name}")
            except Exception:
                pass
        # 2. Clean up any leftover beam-temp-* directories
        elif name.startswith("beam-temp-") and os.path.isdir(path):
            try:
                shutil.rmtree(path, onerror=lambda func, p, _: (os.chmod(p, 0o700), func(p)))
                logging.info(f"🧹 Cleaned up temp directory: {name}")
            except Exception:
                pass
