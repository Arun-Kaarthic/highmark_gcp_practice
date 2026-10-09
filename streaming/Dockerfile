FROM gcr.io/dataflow-templates-base/python311-template-launcher-base

# Beam Python KafkaIO is a cross-language transform; default expansion needs Java.
RUN apt-get update && apt-get install -y --no-install-recommends openjdk-17-jre-headless \
    && rm -rf /var/lib/apt/lists/*
WORKDIR /template
COPY requirements.txt /template/requirements.txt
RUN pip install --no-cache-dir -r /template/requirements.txt
COPY pipeline.py /template/pipeline.py
ENV FLEX_TEMPLATE_PYTHON_PY_FILE=/template/pipeline.py
ENV FLEX_TEMPLATE_PYTHON_REQUIREMENTS_FILE=/template/requirements.txt
