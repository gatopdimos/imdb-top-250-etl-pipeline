# set the base image with specific tag/version
FROM python:3.9

# install system dependencies for cron, vim/nano
RUN apt-get update && apt-get install -y \
    cron \
    vim \
    nano \
    wget \
    gnupg \
    unzip \
    xvfb \
    libxi6 \
    libnss3 \
    libx11-xcb1 \
    libxcomposite1 \
    libxcursor1 \
    libxdamage1 \
    libxrandr2 \
    libasound2 \
    libatk1.0-0 \
    libcups2 \
    libdbus-1-3 \
    libdrm2 \
    libgbm1 \
    libgtk-3-0 \
    libpango-1.0-0 \
    libxss1 \
    libxtst6 \
    fonts-liberation \
    lsb-release \
    xdg-utils \
    --no-install-recommends && rm -rf /var/lib/apt/lists/*

# install google chrome
RUN wget -q -O - https://dl.google.com/linux/linux_signing_key.pub | gpg --dearmor -o /usr/share/keyrings/google-linux-signing-key.gpg \
    && echo "deb [arch=amd64 signed-by=/usr/share/keyrings/google-linux-signing-key.gpg] http://dl.google.com/linux/chrome/deb/ stable main" \
       > /etc/apt/sources.list.d/google-chrome.list \
    && apt-get update \
    && apt-get install -y google-chrome-stable \
    && rm -rf /var/lib/apt/lists/*

# set up working directory inside the container
WORKDIR /app

# copy and run the requirements.txt file to install the required packages.
COPY requirements.txt .
RUN pip3 install -r requirements.txt

# copy files from  directory to the image's filesystem
COPY . .

RUN mkdir -p csv
# register a cron job to start the webscraper application, cron job runs every Sunday at 00:00
RUN crontab -l | { cat; echo "0 0 * * 0 /usr/local/bin/python /app/webscraper-postgres.py  > /proc/1/fd/1 2>&1"; } | crontab -

# start cron in foreground and set it as executable command for when the container starts 
CMD ["cron", "-f"]