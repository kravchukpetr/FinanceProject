# Use the official Python image as a base
FROM python:3.9-slim

# Install Ansible and any other required packages
# Install Ansible and OpenSSH client
RUN apt-get update && \
    apt-get install -y \
    openssh-client \
    && pip install ansible \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Create a directory for Ansible playbooks and set it as the working directory
WORKDIR /ansible

# Copy the local Ansible playbooks and inventory to the container
COPY ./playbooks /ansible/playbooks
COPY ./inventory /ansible/inventory

# Copy the SSH private key into the container
COPY ./.ssh/id_rsa_root /root/.ssh/id_rsa
COPY ./.ssh/id_rsa_root.pub /root/.ssh/id_rsa.pub

# Set permissions for the SSH key
RUN chmod 600 /root/.ssh/id_rsa && \
    chmod 644 /root/.ssh/id_rsa.pub && \
    mkdir -p /root/.ssh && \
    touch /root/.ssh/known_hosts

ENV ANSIBLE_SSH_ARGS="-o StrictHostKeyChecking=no"
CMD ["ansible-playbook", "-i", "inventory/hosts.ini", "playbooks/deploy_docker_compose.yml"]
CMD ["ansible-playbook", "-i", "inventory/hosts.ini", "playbooks/backup_postgres.yml"]
# Entry point to keep the container running
CMD ["tail", "-f", "/dev/null"]
