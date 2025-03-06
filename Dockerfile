FROM python:3.13-slim-bookworm

COPY --from=ghcr.io/astral-sh/uv:latest /uv /uvx /bin/

# Prevents Python from writing pyc files.
ENV PYTHONDONTWRITEBYTECODE=1

# Keeps Python from buffering stdout and stderr to avoid situations where
# the application crashes without emitting any logs due to buffering.
ENV PYTHONUNBUFFERED=1

COPY . /app
# Sync the project into a new environment, using the frozen lockfile
ENV PATH=/app/bin:$PATH

WORKDIR /app

RUN uv sync --frozen

# ENTRYPOINT [ "tail", "-f", "/dev/null" ]
CMD ["uv", "run", "main.py"]