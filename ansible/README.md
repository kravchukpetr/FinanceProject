### Add ansible use to remote server
```bash
ssh your_existing_user@your_remote_server_ip
sudo adduser ansible
sudo usermod -aG sudo ansible
ssh-keygen -t rsa -b 2048 -f ~/.ssh/id_rsa_ansible
ssh-copy-id -i ~/.ssh/id_rsa_ansible.pub ansible@your_remote_server_ip
ssh your_existing_user@your_remote_server_ip
sudo mkdir -p /home/ansible/.ssh
sudo chmod 700 /home/ansible/.ssh
sudo chown ansible:ansible /home/ansible/.ssh
sudo bash -c 'echo "your_public_key_content" >> /home/ansible/.ssh/authorized_keys'
sudo chmod 600 /home/ansible/.ssh/authorized_keys
sudo chown ansible:ansible /home/ansible/.ssh/authorized_keys
ssh -i ~/.ssh/id_rsa_ansible ansible@your_remote_server_ip
```

### Run ansible container
```bash
docker build -t ansible-container .
docker run -it --name ansible-container -v $(pwd)/playbooks:/ansible/playbooks -v $(pwd)/inventory:/ansible/inventory ansible-container
docker run -it --name ansible-container -v ./playbooks:/ansible/playbooks -v ./inventory:/ansible/inventory -v ./.ssh:/ansible/ssh ansible-container
```
### In docker container
```bash
cd /ansible
ansible-playbook -i inventory/hosts.ini playbooks/deploy_docker_compose.yml
ansible-playbook -i inventory/hosts.ini playbooks/deploy_docker_compose.yml --ask-become-pass
```
Backup to crontab
```bash
ansible-playbook -i inventory/hosts.ini playbooks/backup_postgres.yml --ask-become-pass
```


