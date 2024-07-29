# Use an official Python runtime as a parent image
FROM python:3.9-slim

# Set the working directory in the container
WORKDIR /docker_hosting

# Copy the current directory contents into the container at /app
COPY . /docker_hosting



# Make port 80 available to the world outside this container
EXPOSE 80

# Command to run the application
CMD ["python", "python_test_loki_script.py"]
