FROM node:14.17 as frontend-builder

RUN npm install --global --force yarn@1.22.19


# Controls whether to build the frontend assets
ARG skip_frontend_build
ENV CYPRESS_INSTALL_BINARY=0
ENV PUPPETEER_SKIP_CHROMIUM_DOWNLOAD=1

RUN useradd -m -d /frontend holmes
USER holmes

WORKDIR /frontend
COPY --chown=holmes package.json yarn.lock .yarnrc /frontend/
COPY --chown=holmes client /frontend/client
COPY --chown=holmes package.json /frontend/
COPY --chown=holmes webpack.config.js /frontend/
COPY --chown=holmes .env /frontend/

# Controls whether to instrument code for coverage information
ARG code_coverage
ENV BABEL_ENV=${code_coverage:+test}
# limit requires network concurrency to 1
# RUN yarn install --network-concurrency 1;

RUN yarn && yarn build

FROM python:3.8.18-slim
##  out port
EXPOSE 8338 8339
# Controls whether to install extra dependencies needed for all data sources.
ARG skip_ds_deps=true
# Controls whether to install dev dependencies. If need, set skip_dev_deps eq ''
ARG skip_dev_deps=yes
## Create user
RUN useradd --create-home holmes
## Ubuntu use aliyun source
RUN apt-get update
## Ubuntu packages
RUN apt-get clean && apt-get update && \
  apt-get install -y --no-install-recommends \
  curl \
  gnupg \
  build-essential \
  pwgen \
  libffi-dev \
  sudo \
  git-core \
  # Postgres client
  libpq-dev \
  # ODBC support:
  g++ unixodbc-dev \
  # for SAML
  xmlsec1 \
  # Additional packages required for data sources:
  libssl-dev \
  default-libmysqlclient-dev \
  freetds-dev \
  libsasl2-dev \
  unzip \
  libsasl2-modules-gssapi-mit && \
  apt-get clean && \
  rm -rf /var/lib/apt/lists/*
#
# ali pip source
RUN pip config set global.index-url https://pypi.tuna.tsinghua.edu.cn/simple
RUN pip install --upgrade pip
COPY --chown=holmes . /app
WORKDIR /app
## Disable PIP Cache and Version Check
ENV PIP_DISABLE_PIP_VERSION_CHECK=1
ENV PIP_NO_CACHE_DIR=1
## copy webpack build file to image
COPY --from=frontend-builder /frontend/client/dist /app/client/dist
## rollback pip version to avoid legacy resolver problem
RUN pip install pip==20.2.4;
## We first copy only the requirements file, to avoid rebuilding on every file change.
RUN if [ "x$skip_dev_deps" = "x" ] ; then pip install -r requirements_dev.txt ; fi
#
RUN pip install -r requirements.txt && pip install -r requirements_ai.txt
#
## fix python 3.8.18 error import .
RUN sed -i 's/from importlib_resources import path/from importlib.resources import path/g' /usr/local/lib/python3.8/site-packages/saml2/sigver.py && \
    sed -i 's/from importlib_resources import path/from importlib.resources import path/g' /usr/local/lib/python3.8/site-packages/saml2/xml/schema/__init__.py && \
    chown holmes /app && chmod +x /app/ai/main.py

ENTRYPOINT ["/app/bin/docker-entrypoint"]
CMD ["server"]

