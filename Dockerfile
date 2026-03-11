FROM python:3.12-slim-trixie

COPY --from=ghcr.io/astral-sh/uv:latest /uv /uvx /bin/

ENV UV_NO_DEV=1

WORKDIR /app

# copy dependency files trước
COPY . /app

RUN uv sync --no-dev --no-cache --frozen

# sau đó mới copy code

CMD ["sh", "-c", "uv run streamlit run main.py \
  --server.address=${STREAMLIT_HOST} \
  --server.port=${STREAMLIT_PORT}"]
