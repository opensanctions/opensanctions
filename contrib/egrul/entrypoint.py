"""Stub for `gcloud dataproc batches submit pyspark`, which requires a
main_python_file_uri. All real code lives in the container image (on
PYTHONPATH via the Dockerfile). This file gets uploaded once to GCS and
doesn't need to change when generate.py does — rebuild the image instead.
"""

from generate import main

if __name__ == "__main__":
    main()
