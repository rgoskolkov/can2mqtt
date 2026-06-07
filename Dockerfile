# Use the new Home Assistant base image
FROM ghcr.io/home-assistant/base:latest

# Set up shell
SHELL ["/bin/bash", "-o", "pipefail", "-c"]

# Install build dependencies and tools needed for the addon
# git is required for canopen-async dependency
# can-utils is useful for debugging
RUN apk add --no-cache git python3 py3-pip eudev-dev g++ make can-utils iproute2

# Set the working directory
WORKDIR /usr/src/app

# Copy project files
COPY pyproject.toml ./
COPY src/ ./src/

# Install the project and its dependencies
# This will also create the `canopen2HAmqtt` executable
RUN pip3 install . --no-cache-dir --break-system-packages

# Copy the run script
COPY run.sh ./

# Make run.sh executable
RUN chmod a+x run.sh

# This will be executed by the Home Assistant Supervisor
CMD [ "/usr/src/app/run.sh" ]
