import os
import shutil
import logging

LOCAL_DIRECT_RUNNER = "apache_beam.runners.direct.direct_runner.BundleBasedDirectRunner"


def setup_logging():
    """Configure logging for both local and cloud execution."""
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s [%(levelname)s] %(message)s',
        datefmt='%H:%M:%S'
    )


def normalize_beam_args(beam_args):
    """Use Beam's bundle-based DirectRunner on Windows-friendly local runs."""
    normalized = []
    skip_next = False

    for index, arg in enumerate(beam_args):
        if skip_next:
            skip_next = False
            continue

        if arg == "--runner" and index + 1 < len(beam_args):
            runner = beam_args[index + 1]
            normalized.extend([arg, LOCAL_DIRECT_RUNNER if runner == "DirectRunner" else runner])
            skip_next = True
        elif arg.startswith("--runner="):
            runner = arg.split("=", 1)[1]
            normalized.append(f"--runner={LOCAL_DIRECT_RUNNER}" if runner == "DirectRunner" else arg)
        else:
            normalized.append(arg)

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
                logging.info("Cleaned up metadata: %s", name)
            except Exception:
                pass
        # 2. Clean up any leftover beam-temp-* directories
        elif name.startswith("beam-temp-") and os.path.isdir(path):
            try:
                shutil.rmtree(path, onerror=lambda func, p, _: (os.chmod(p, 0o700), func(p)))
                logging.info("Cleaned up temp directory: %s", name)
            except Exception:
                pass
