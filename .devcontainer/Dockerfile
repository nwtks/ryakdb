FROM mcr.microsoft.com/dotnet/core/sdk:3.1

ARG USERNAME=vscode
ARG USER_UID=1000
ARG USER_GID=$USER_UID

RUN apt-get update \
    && export DEBIAN_FRONTEND=noninteractive \
    && apt-get install -y --no-install-recommends apt-utils dialog 2>&1 \
    && apt-get install -y --no-install-recommends git openssh-client less iproute2 procps apt-transport-https gnupg2 curl lsb-release \
    && groupadd --gid $USER_GID $USERNAME \
    && useradd -s /bin/bash --uid $USER_UID --gid $USER_GID -m $USERNAME \
    && apt-get install -y --no-install-recommends sudo \
    && echo $USERNAME ALL=\(root\) NOPASSWD:ALL > /etc/sudoers.d/$USERNAME\
    && chmod 0440 /etc/sudoers.d/$USERNAME

ENV PATH "$PATH:/home/$USERNAME/.dotnet/tools"
