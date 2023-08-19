FROM tailucas/base-app:20230815_3
# for system/site packages
USER root
# system setup
RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        html-xml-utils \
        sqlite3
# user scripts
COPY backup_db.sh .
# cron jobs
RUN rm -f ./config/cron/base_job
COPY config/cron/backup_db ./config/cron/
# apply override
RUN /opt/app/app_setup.sh
# tools
COPY investec-api-python/ ./investec-api-python/
# refresh pylib from base
COPY pylib/ ./pylib/
# switch to user
USER app
# override configuration
COPY config/app.conf ./config/app.conf
COPY poetry.lock pyproject.toml ./
RUN /opt/app/python_setup.sh
# add the project application
COPY app/ ./app/
# override entrypoint
COPY app_entrypoint.sh .
CMD ["/opt/app/entrypoint.sh"]
