# Jasmin Submit Logs Service

after download it create new virtual environment and install the required dependencies:

```sh
virtualenv -p python3 env
source env/bin/activate
pip install -r requirements.txt
```

copy the `.env` file from `Sample.env` and ensure the required variables for database communication.

```sh
cp Sample.env .env
```

Adding service to systemd

```sh
ln -s /jasmin/jasmin-submit-logs/sms_logger.service /etc/systemd/system/
```

Enable and Start Systemctl service

```sh
systemctl daemon-reload
systemctl enable sms_logger.service
systemctl start sms_logger.service
```

to check service up-and-running

```sh
systemctl status sms_logger.service
```
