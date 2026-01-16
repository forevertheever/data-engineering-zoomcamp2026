# data-engineering-zoomcamp2026
This is a personal project where i learn the key skills for data engineering. The data domain for implementing a pipeline will be specified later.

## Homework Week1

Q1. Run docker with the python:3.13 image. Use an entrypoint bash to interact with the container. What's the version of pip in the image?

Answer: 25.3
Solution: Running the following code in the terminal to establish a python container in docker with version python:3.13
```bash
docker run -it \
    --rm \
    --entrypoint=bash \
    python:3.13
```
and then run 'pip -v' to check the pip version, which shows 25.3.