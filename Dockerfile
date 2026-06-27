FROM python:3.12-slim

WORKDIR /code

COPY pyproject.toml /code/

RUN mkdir -p orion && touch orion/__init__.py
RUN pip install --no-cache-dir --no-build-isolation -e .

COPY orion /code/orion

EXPOSE 8000

CMD ["uvicorn", "orion.main:app", "--host", "0.0.0.0", "--port", "8000"]
