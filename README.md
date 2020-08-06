# zoom
Data pipeline for Zoom participant data to monitor student activity

## Dependencies:

- Python3.7
- [Pipenv](https://pipenv.readthedocs.io/en/latest/)
- [Docker](https://www.docker.com/)

## Getting Started

### Setup Environment

1. Clone this repo

```
git clone https://github.com/kipp-bayarea/zoom.git
```

2. Install dependencies

- Docker can be installed directly from the website at docker.com.

3. Create .env file with project secrets

```
# Zoom Auth Credentials
ZOOM_KEY=
ZOOM_SECRET=

# Database Credentials
DB_TYPE=
DB_SERVER=
DB=
DB_SCHEMA=
DB_PORT=
DB_USER=
DB_PWD=

# Email Credentials (Optional)
ENABLE_MAILER=
SENDER_EMAIL=
SENDER_PWD=
RECIPIENT_EMAIL=

# Enable Debug Logging (Optional)
DEBUG_MODE=

# Database Credentials
DB=
DB_SERVER=
DB_USER=
DB_PWD=
DB_SCHEMA=

# Email Notifications
ENABLE_MAILER=1
SENDER_EMAIL=
SENDER_PWD=
RECIPIENT_EMAIL=
```

4. Build the container

```
$ docker build -t zoom .
```


5. Running the job

```
$ docker run --rm -it zoom
```