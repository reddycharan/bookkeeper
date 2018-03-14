FROM ops0-artifactrepo1-0-prd.data.sfdc.net/docker-dev-base/tnrp/centos:7

ARG pipeline_args
ARG version

LABEL maintainer="Storage Cloud Foundation \"storagefoundation@salesforce.com\"" \
      pipeline=$pipeline_args \
      version=$version \
      name=storagecloud/sfstore:latest

ADD sfstore.tar.gz /sfs/sfsbuild

# Install necessary packages
RUN touch /var/lib/rpm/* && \
    yum install -y --nogpgcheck which java bzip2 wget net-tools nmap-ncat && \
    yum clean all && \
    set -x && \
    mkdir -p /sfs/sfsbuild /sfs/sfsdata /sfs/sfslogs/ /sfs/sfsdata/journal /sfs/sfsdata/ledger && \
    groupadd --system --gid 7447 sfstore && \
    adduser --system --gid 7447 --uid 7447 sfstore && \
    chown -R sfstore:sfstore /sfs

USER sfstore
