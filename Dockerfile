FROM ubuntu:24.04

# non interactive due to docker
ENV DEBIAN_FRONTEND=noninteractive

# installing libs and isolate too
RUN apt-get update && apt-get install -y \
    wget \
    curl \
    pkg-config \
    libcap-dev \
    libseccomp-dev && \
    curl https://www.ucw.cz/isolate/debian/signing-key.asc > /etc/apt/keyrings/isolate.asc && \
    echo "Types: deb\nURIs: http://www.ucw.cz/isolate/debian/\nSuites: trixie-isolate\nComponents: main\nArchitectures: arm64\nSigned-By: /etc/apt/keyrings/isolate.asc" > /etc/apt/sources.list.d/isolate.sources && \
    apt-get update && \
    apt-get install -y isolate

# installing go in the image    
RUN wget https://go.dev/dl/go1.24.3.linux-arm64.tar.gz && \
    tar -C /usr/local -xzf go1.24.3.linux-arm64.tar.gz && \
    rm go1.24.3.linux-arm64.tar.gz


# setting the path
ENV PATH=$PATH:/usr/local/go/bin

# set working directory
WORKDIR /app

# copy go mod files first (for layer caching)
COPY go.mod go.sum ./
RUN go mod download

# copy the rest of the source
COPY . .

# build the binary
RUN go build -o nexus ./cmd/nexus

# run the binary
ENTRYPOINT ["./nexus"]