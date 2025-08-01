# syntax=docker/dockerfile:1.4

############################
# Base image with micromamba
############################
FROM mambaorg/micromamba:1.5.6-bullseye-slim as base

# Copy and install the conda environment (better caching)
COPY environment.yml /tmp/environment.yml

# Install all dependencies into a dedicated env called "Scigility" (use YAML spec)
RUN micromamba env create -n Scigility -f /tmp/environment.yml \
    && micromamba clean --all --yes

# Activate the environment by default for every RUN / CMD / ENTRYPOINT layer
ENV MAMBA_DOCKERFILE_ACTIVATE=1
ENV PATH=/opt/conda/envs/Scigility/bin:$PATH
ENV CONDA_DEFAULT_ENV=Scigility

# JAVA_HOME is required by PySpark
ENV JAVA_HOME=/opt/conda/envs/Scigility

#########################
# Runtime image
#########################
FROM base as runtime

# Need root privileges to create a non-root runtime user
USER root

# Create an unprivileged user for security best practices
RUN useradd --create-home --shell /bin/bash appuser

# Set working directory
WORKDIR /app

# Copy the repository
COPY . /app

# Adjust ownership
RUN chown -R appuser:appuser /app

# Drop privileges
USER appuser

# Default command to run the pipeline
CMD ["python", "-m", "main"] 