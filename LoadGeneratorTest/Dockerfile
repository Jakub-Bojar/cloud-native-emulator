FROM alpine:3.19

# Install our two core tools
RUN apk add --no-cache stress-ng iperf3

# Copy the startup script and make it executable
COPY start.sh /app/start.sh
RUN chmod +x /app/start.sh

WORKDIR /app

# Run the script when the container starts
CMD ["/app/start.sh"]